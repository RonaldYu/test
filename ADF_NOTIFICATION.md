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
        and Level == "Error"
        and TimeGenerated > ago(24h)
| summarize dcount(CorrelationId)
```
