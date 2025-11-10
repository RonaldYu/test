

docker run --rm -it 0d0f9f5b7ecf127928bea5bb68d307a9fcd79ecd2188e3a1c9d4b1d9ad3f66f1 bash

az login --tenant d1289755-d641-4ba1-8d33-fe2918e71872
az config set extension.use_dynamic_install=yes_without_prompt;

adf_name=sdafsadfadf
adf_resource_group=test-rg
adf_pipeline_pth=pipeline1.json
pipeline_nm=$(cat $adf_pipeline_pth | jq -r '.name')
pipeline_pr=$(cat $adf_pipeline_pth | jq -c '.properties')



existing_pipeline=$(az datafactory pipeline show --factory-name sdafsadfadf --name aipil-datapipe-000-003-000_overallorch --resource-group test-rg --query "name" -o tsv 2>/dev/null || echo "NotFound")

az datafactory pipeline create --factory-name $adf_name --pipeline "$pipeline_pr" --name $pipeline_nm --resource-group $adf_resource_group
az datafactory pipeline delete --factory-name $adf_name --name $pipeline_nm --resource-group $adf_resource_group --yes

adf_trigger_pth="datapipe-overallorch-proc-info-alert1.json"
password_placeholder="PASSWORD"
trigger_nm=$(cat $adf_trigger_pth | jq -r '.name')
trigger_pr=$(cat $adf_trigger_pth | jq -c '.properties')

# Inject sensitive values from environment variables if provided
# Example: export ADF_PASSWORD=your-secret
ADF_PASSWORD="secret"
if [ -n "${ADF_PASSWORD:-}" ]; then
    echo "ADF_PASSWORD is set"
    # trigger_pr=$(echo "$trigger_pr" | jq --arg pwd "$ADF_PASSWORD" -c '(.pipelines[]?.parameters.password) |= $pwd')
    
    # jq --arg pwd "$ADF_PASSWORD" \
    #    '(.. | select(type=="string")) |= gsub("\\$PASSWORD"; $pwd)' \
    #    $adf_trigger_pth > resolved_$adf_trigger_pth

    trigger_pr=$(printf '%s' "$trigger_pr" | jq --arg pwd "$ADF_PASSWORD" --arg pwd_hlr "$password_placeholder" -c \
        '(.. | select(type=="string")) |= gsub("\\$" + $pwd_hlr; $pwd)')

fi

# Stop trigger if it's currently started to allow update
existing_state=$(az datafactory trigger show --factory-name $adf_name --name $trigger_nm --resource-group $adf_resource_group --query "properties.runtimeState" -o tsv 2>/dev/null || echo "NotFound")
if [ "$existing_state" = "Started" ]; then
az datafactory trigger stop --factory-name $adf_name --name $trigger_nm --resource-group $adf_resource_group
fi

az datafactory trigger create --factory-name $adf_name --properties "$trigger_pr" --name $trigger_nm --resource-group $adf_resource_group

# Re-start trigger if it was previously started
if [ "$existing_state" = "Started" ]; then
az datafactory trigger start --factory-name $adf_name --name $trigger_nm --resource-group $adf_resource_group
fi
az datafactory trigger delete --factory-name $adf_name --name $trigger_nm --resource-group $adf_resource_group --yes






deploy_output=$(az deployment group create --resource-group test-rg  --template-file overallorch-fail-alert.json --name overallorch-fail-alert2 2>&1);

cancel_output=$(az deployment group cancel --name overallorch-fail-alert2 --resource-group test-rg 2>&1);

az deployment group create --resource-group test-rg  --template-file overallorch-fail-alert.json --name overallorch-fail-alert2 --no-wait

az deployment group cancel --name overallorch-fail-alert2 --resource-group test-rg


az deployment group show  --name overallorch-fail-alert2 --resource-group test-rg

az deployment group delete --name overallorch-fail-alert2 --resource-group test-rg


delete_output=$(az monitor scheduled-query delete --name aipil-alert-rule-test2 --resource-group test-rg --yes 2>&1);
if [ "$delete_output" = "" ]; then
    echo "Success"
else
    echo "Failed"
fi
check_output=$(az monitor scheduled-query show --name aipil-alert-rule-test2 --resource-group test-rg 2>&1);
az monitor scheduled-query show --name aipil-alert-rule-test2 --resource-group test-rg

check_output=$(az monitor scheduled-query show --name aipil-alert-rule-test2 --resource-group test-rg 2>&1);
alert_name=$(echo $check_output | jq -r '.name')
echo "Alert name: $alert_name"