Synthetic function for reporting
1) connects to an API (only one api call to start)
    - input client id
    - input client secret
    - input api uri
    - input scopes
2) email processing & templating
    - conditional filtering: applies filtering on the dataset retrieved
    - data columns you need
    - data enrichment: matchB
    - recipients matrix: generate email jobs for email + column(s)/value pairs
    - default recipients: generate email job without applying any filtering
    - templating
        - output: data table, pandas summarized dataset