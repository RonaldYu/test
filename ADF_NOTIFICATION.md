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



