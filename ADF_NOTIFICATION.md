# Preparation
## Set up Action Group in Azure Monitor
- Azure Portal > Monitor > Alerts (left-side bar) > + Create > Action group
- In Action group creation, add notification method


## Assign a role to SP in Azure Log Analytics
- require role: Monitoring Contributor
    - 建立與管理警示規則
    - 建立與管理 Action Groups
    - 查看監控資料（但無法修改 Log Analytics 工作區設定）


## Assign Log Analytics Workspace in Azure Data Factory
- Azure Portal > Azure Data Factory > Monitoring > Diagnostic > + Add diagnostic setting


## Create Alert rule in Azure Monitor
- Azure Portal > Monitor > Alerts (left-side bar) > + Create > Alert rule
- In Alert rule creation,
    - Scope
        - select scope level > click the log analytics workspace
    - Condition
        - Signal name: Custom log search
        - Query type: Aggregated logs
        - Search query:
            ```kql
            ADFPipelineRun
            | where Annotations has "aipil"
                and Annotations has "data-pipe"
                and Annotations has "pipeline-orchestration"
                and Status == "Failed"
                and TimeGenerated > ago(30m)

            ```
        - Measurement:
            - Measure: Table rows
            - Aggregation type: count
            - Aggregation granularity: 10 mins
        - Split by dimensions (Optional)
        - Alert logic:
            - Operator: Greater than
            - Threashold value: 0
            - Frequency evaluation: 10 minutes
        - Avanced options (Number of vilations to trigger the alert):
            - Number of violations: 1
            - Evaluation period: 1 hour
    - Actions
        - + Select action groups
            - add the action group created before
        - Email subject
            - add the email subject
    - Detail
        - Alert rule defails
            - Severity

# Referecne
## KQL in Azure Log Analytics Workspace
```kql
### Write KQL query 
ADFActivityRunADFPipelineRun 
| extend userProps = todynamic(UserProperties)
| where userProps['project-name'] == "aipil"
        and userProps['service-name'] == "data-pipe"
        and userProps['job-name'] == "pipeline-orchestration"
        and tostring(PipelineName)  == "aipil-datapipe-000-003-000_overallorch"
```

```kql
ADFPipelineRun 
| where Annotations has "aipil"
        and Annotations has "data-pipe"
        and Annotations has "pipeline-orchestration"
        and Type == "ADFPipelineRun"
        and Status == "Failed"
        and TimeGenerated > ago(24h)
| summarize dcount(CorrelationId)
```

## Explanation
My setting likes below:
- KQL
    ```kql
    ADFPipelineRun 
    | where Annotations has "aipil" 
        and Annotations has "data-pipe" 
        and Annotations has "pipeline-orchestration" 
        and Status == "Failed"
    ```
- Measurement
    - Measure = Table rows 
    - Aggregation type = Count 
    - Aggregation granularity = 5 minutes

- Alert logic
    - Operator = Greater than
    - Threshold value = 0
    - Frequency of evaluation = 5 minutes
- Advanced options
    - Number of violations = 1 
    - Evaluation period = 30 minutes
    - Override query time range = None (30minutes)

Azure Monitor 的警示規則會依照以下邏輯運作。這是一個非常典型且實用的設計，適合監控 ADF pipeline 的失敗情況。我會從查詢執行流程、違規判斷、警示觸發條件三個層面完整說明：

---
### 🧠 1. 查詢執行流程

| 項目 | 設定值 | 說明 |
|------|--------|------|
| **Frequency of evaluation** | 5 分鐘 | 每 5 分鐘執行一次警示查詢 |
| **Evaluation period** | 30 分鐘 | 每次查詢時，回溯過去 30 分鐘的資料 |
| **Override query time range** | 關閉 | Azure Monitor 會自動在查詢中加上 `TimeGenerated > ago(30m)` |

✅ 查詢範圍範例（假設現在時間為 10:00）：
- 查詢時間：10:00
- 查詢資料時間範圍：**09:30 – 10:00**
- 查詢語句實際執行時會變成：
  ```kql
  ADFPipelineRun
  | where TimeGenerated > ago(30m)
      and Annotations has "aipil"
      and Annotations has "data-pipe"
      and Annotations has "pipeline-orchestration"
      and Status == "Failed"
  ```
---
### 📊 2. 查詢結果與彙總邏輯

| 項目 | 設定值 | 說明 |
|------|--------|------|
| **Measure** | Table rows | 以資料筆數為計算單位 |
| **Aggregation type** | Count | 計算符合條件的筆數 |
| **Aggregation granularity** | 5 分鐘 | 將資料以每 5 分鐘為單位進行分段統計（bin） |

✅ 查詢結果範例（假設有資料）：

| TimeGenerated bin | Failed count |
|-------------------|--------------|
| 09:35 – 09:40     | 0            |
| 09:40 – 09:45     | 1            |
| 09:50 – 09:55     | 2            |

---

### ⚠️ 3. 警示邏輯與觸發條件

| 項目 | 設定值 | 說明 |
|------|--------|------|
| **Operator** | Greater than |
| **Threshold value** | 0 |
| **Number of violations** | 1 |

✅ 警示觸發條件：

- 在查詢結果中，**有任一個 5 分鐘 bin 的筆數 > 0**（即有至少一筆失敗紀錄），就算一次違規。
- 因為你設定 **Number of violations = 1**，只要有一次違規就會觸發警示。

---

🔁 整體流程總結

1. **每 5 分鐘**，Azure Monitor 執行一次查詢。
2. 查詢會回溯 **過去 30 分鐘** 的資料。
3. 系統會將資料以 **每 5 分鐘為單位** 分段統計失敗次數。
4. 如果 **任一個時間段的失敗數 > 0**，就算違規。
5. 因為你設定 **違規次數 = 1**，所以只要有一段時間出現失敗，就會立即觸發警示。

---

✅ 建議補充

- 若你擔心 ingestion 延遲導致漏報，可考慮將 `Evaluation period` 設為 35–45 分鐘。
- 若你想避免重複通知，可在警示規則中設定 **Suppression（抑制時間）**，例如 30 分鐘內不重複觸發。

---

### 使用 split_by_dimensions 的影響

#### 📌 當加入 Resource ID 和 Run ID 作為維度時

如果你在 **Split by dimensions** 中加入了：
- **Resource ID column**（資源識別碼欄位）
- **Run ID**（執行識別碼欄位）

警示規則的行為會發生以下重要變化：

---

#### 🔄 1. 資料分組邏輯的改變

**原本（無 split_by_dimensions）：**
- 所有符合條件的失敗 pipeline runs 會被**彙總在一起**計算
- 查詢結果只有一個總計數值

**加入維度後：**
- 資料會按照 **Resource ID + Run ID** 的組合進行**分組**
- 每個組合會被**獨立計算**和評估

---

#### 📊 2. 查詢結果結構的變化

**原本的查詢結果範例：**

| TimeGenerated bin | Failed count |
|-------------------|--------------|
| 09:35 – 09:40     | 0            |
| 09:40 – 09:45     | 1            |
| 09:50 – 09:55     | 2            |

**加入維度後的查詢結果範例：**

| TimeGenerated bin | Resource ID | Run ID | Failed count |
|-------------------|-------------|--------|--------------|
| 09:35 – 09:40     | /subscriptions/.../factories/adf-001 | run-001 | 0 |
| 09:40 – 09:45     | /subscriptions/.../factories/adf-001 | run-001 | 1 |
| 09:40 – 09:45     | /subscriptions/.../factories/adf-002 | run-002 | 1 |
| 09:50 – 09:55     | /subscriptions/.../factories/adf-001 | run-001 | 2 |

---

#### ⚠️ 3. 警示觸發邏輯的改變

**重要變化：**

1. **獨立評估**：每個 **Resource ID + Run ID** 組合會被**單獨評估**
   - 每個組合都有自己的計數和閾值檢查
   - 每個組合都獨立判斷是否違規

2. **多個警示實例**：
   - 如果有多個不同的 Resource ID + Run ID 組合都出現失敗（> 0），**每個組合都會觸發一個獨立的警示**
   - 例如：如果有 3 個不同的 pipeline run 失敗，可能會觸發 **3 個警示實例**

3. **警示內容更精確**：
   - 每個警示會包含具體的 **Resource ID** 和 **Run ID** 資訊
   - 可以更精確地識別是哪個資源的哪個執行失敗

---

### 🎯 4. 實際運作範例

假設在 09:30 – 10:00 期間有以下失敗記錄：

| TimeGenerated | Resource ID | Run ID | Status |
|---------------|-------------|--------|--------|
| 09:42 | /subscriptions/.../factories/adf-001 | run-001 | Failed |
| 09:43 | /subscriptions/.../factories/adf-001 | run-002 | Failed |
| 09:52 | /subscriptions/.../factories/adf-002 | run-003 | Failed |

**無 split_by_dimensions：**
- 總計：3 筆失敗
- 觸發：**1 個警示**（因為總數 > 0）

**有 split_by_dimensions（Resource ID + Run ID）：**
- 分組結果：
  - `adf-001 + run-001`: 1 筆失敗 → 觸發警示 #1
  - `adf-001 + run-002`: 1 筆失敗 → 觸發警示 #2
  - `adf-002 + run-003`: 1 筆失敗 → 觸發警示 #3
- 觸發：**3 個獨立的警示實例**

---

### ✅ 5. 使用維度的優缺點

**優點：**
- ✅ **精確定位**：可以清楚知道是哪個資源的哪個執行失敗
- ✅ **獨立追蹤**：每個 pipeline run 的失敗情況被獨立監控
- ✅ **詳細資訊**：警示通知中包含更詳細的上下文資訊

**缺點：**
- ⚠️ **可能產生大量警示**：如果有多個 pipeline runs 同時失敗，會產生多個警示實例
- ⚠️ **通知頻繁**：可能導致通知過多，需要適當設定 Action Group 的抑制機制

---

### 💡 6. 建議配置

如果使用 split_by_dimensions，建議：

1. **設定 Alert Suppression（抑制時間）**：
   - 例如：30 分鐘內不重複觸發同一維度組合的警示
   - 避免同一 pipeline run 的多次失敗產生重複通知

2. **考慮使用 Resource ID 單一維度**：
   - 如果只需要知道哪個 ADF 資源失敗，可以只使用 Resource ID
   - 這樣可以減少警示數量，同時仍能識別失敗的資源

3. **評估通知頻率**：
   - 根據你的業務需求，決定是否需要 Run ID 級別的細粒度監控
   - 如果 pipeline runs 很頻繁，可能會產生大量警示

---



