<#
.SYNOPSIS
    Converts an Entity Relationship Diagram (ERD) file to Dataverse tables and option sets.

.DESCRIPTION
    Standalone script that parses an ERD file in DBDiagram.io format and creates 
    corresponding Dataverse tables, global option sets, and relationships.
    Uses client secret authentication instead of interactive login.

.PARAMETER ErdFilePath
    Path to the ERD file in DBDiagram.io format.

.PARAMETER SolutionName
    The name of the solution to add the created components to.

.PARAMETER PublisherPrefix
    The publisher prefix to use for schema names.

.PARAMETER OrganizationUrl
    The URL of the Dataverse organization.

.PARAMETER TenantId
    The Azure AD tenant ID.

.PARAMETER ClientId
    The application (client) ID for the app registration.

.PARAMETER ClientSecret
    The client secret for the app registration.

.PARAMETER WhatIf
    If specified, shows what would happen but doesn't make any changes.

.EXAMPLE
    .\ClientSecret-ERD-to-Dataverse.ps1 -ErdFilePath ".\CM-ERD.txt" -SolutionName "MyERDSolution" -PublisherPrefix "crd" -OrganizationUrl "https://myorg.crm.dynamics.com" -TenantId "your-tenant-id" -ClientId "your-client-id" -ClientSecret "your-client-secret"
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$ErdFilePath,
    
    [Parameter(Mandatory = $true)]
    [string]$SolutionName,
    
    [Parameter(Mandatory = $true)]
    [string]$PublisherPrefix,
    
    [Parameter(Mandatory = $true)]
    [string]$OrganizationUrl,
    
    [Parameter(Mandatory = $true)]
    [string]$TenantId,
    
    [Parameter(Mandatory = $true)]
    [string]$ClientId,
    
    [Parameter(Mandatory = $true)]
    [string]$ClientSecret,
    
    [Parameter(Mandatory = $false)]
    [switch]$WhatIf = $false
)

# Ensure publisher prefix ends with underscore
if (-not $PublisherPrefix.EndsWith("_")) {
    $PublisherPrefix = "$PublisherPrefix" + "_"
}

#region Core functions

# Set to $true only while debugging with Fiddler
$debug = $false
# Set this value to the Fiddler proxy URL configured on your computer
$proxyUrl = 'http://127.0.0.1:8888'

$baseHeaders = @{}
$baseURI = ""

# Connect function using client secret
function Connect-Dataverse {
    param (
        [Parameter(Mandatory)] 
        [String] 
        $uri,
        [Parameter(Mandatory)]
        [string]
        $tenantId,
        [Parameter(Mandatory)]
        [string]
        $clientId,
        [Parameter(Mandatory)]
        [string]
        $clientSecret
    )

    Write-Host "Getting access token for $uri..." -ForegroundColor Cyan

    # Build token request
    $tokenUrl = "https://login.microsoftonline.com/$tenantId/oauth2/token"
    $body = @{
        grant_type    = "client_credentials"
        client_id     = $clientId
        client_secret = $clientSecret
        resource      = $uri
    }

    try {
        # Get token
        $response = Invoke-RestMethod -Method Post -Uri $tokenUrl -Body $body -ContentType "application/x-www-form-urlencoded"
        $token = $response.access_token

        # Define common set of headers
        $script:baseHeaders = @{
            'Authorization'    = 'Bearer ' + $token
            'Accept'           = 'application/json'
            'OData-MaxVersion' = '4.0'
            'OData-Version'    = '4.0'
        }

        # Set baseURI
        $script:baseURI = $uri + 'api/data/v9.2/'
        
        Write-Host "Connected to Dataverse at $uri" -ForegroundColor Green
    }
    catch {
        Write-Host "Failed to connect to Dataverse: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

# Invoke Resilient REST Method function
function Invoke-ResilientRestMethod {
    param (
        [Parameter(Mandatory)] 
        $request,
        [bool]
        $returnHeader = $false
    )

    if ($debug) {
        $request.Add('Proxy', $proxyUrl)
    }
    try {
        if ($returnHeader) {
            Invoke-RestMethod @request -ResponseHeadersVariable rhv | Out-Null
            return $rhv
        }
        Invoke-RestMethod @request
    }
    catch [Microsoft.PowerShell.Commands.HttpResponseException] {
        #Write-Host "HTTP Response Error: $_.Exception.Message "
        $statuscode = $_.Exception.Response.StatusCode
        # 429 errors only
        if ($statuscode -eq 'TooManyRequests') {
            if (!$request.ContainsKey('MaximumRetryCount')) {
                $request.Add('MaximumRetryCount', 3)
                # Don't need - RetryIntervalSec
                # When the failure code is 429 and the response includes the Retry-After property in its headers, 
                # the cmdlet uses that value for the retry interval, even if RetryIntervalSec is specified
            }
            # Will attempt retry up to 3 times
            if ($returnHeader) {
                Invoke-RestMethod @request -ResponseHeadersVariable rhv | Out-Null
                return $rhv
            }
            Invoke-RestMethod @request
        }
        else {
            throw $_
        }
    }
    catch {
        throw $_
    }
}

# Invoke DataverseCommands function
function Invoke-DataverseCommands {
    param (
        [Parameter(Mandatory)] 
        $commands
    )
    try {
        Invoke-Command $commands -NoNewScope
    }
    catch [Microsoft.PowerShell.Commands.HttpResponseException] {
        Write-Host "An error occurred calling Dataverse:" -ForegroundColor Red
        $statuscode = [int]$_.Exception.Status;
        $statusText = $_.Exception.Response.StatusCode
        Write-Host "StatusCode: $statuscode ($statusText)"
        # Replaces escaped characters in the JSON
        if ($_.ErrorDetails.Message) {
            [Regex]::Replace($_.ErrorDetails.Message, "\\[Uu]([0-9A-Fa-f]{4})", 
                { [char]::ToString([Convert]::ToInt32($args[0].Groups[1].Value, 16)) } )
        }
    }
    catch {
        Write-Host "An error occurred in the script:" -ForegroundColor Red
        $_
    }
}

# Function to create a new record in Dataverse
function New-Record {
    param (
        [Parameter(Mandatory)] 
        [String] 
        $setName,
        [Parameter(Mandatory)] 
        [hashtable]
        $body
    )

    $postHeaders = $baseHeaders.Clone()
    $postHeaders.Add('Content-Type', 'application/json')
    
    $CreateRequest = @{
        Uri     = $baseURI + $setName
        Method  = 'Post'
        Headers = $postHeaders
        Body    = ConvertTo-Json $body -Depth 10
    }
    
    $rh = Invoke-ResilientRestMethod -request $CreateRequest -returnHeader $true
    $url = $rh['OData-EntityId']
    $selectedString = Select-String -InputObject $url -Pattern '(?<=\().*?(?=\))'
    return [System.Guid]::New($selectedString.Matches.Value.ToString())
}

# Function to get a collection of records from Dataverse
function Get-Records {
    param (
        [Parameter(Mandatory)] 
        [String] 
        $setName,
        [Parameter(Mandatory)] 
        [String] 
        $query
    )
    $uri = $baseURI + $setName + $query
    # Header for GET operations that have annotations
    $getHeaders = $baseHeaders.Clone()
    $getHeaders.Add('If-None-Match', $null)
    $getHeaders.Add('Prefer', 'odata.include-annotations="*"')
    $RetrieveMultipleRequest = @{
        Uri     = $uri
        Method  = 'Get'
        Headers = $getHeaders
    }
    Invoke-ResilientRestMethod $RetrieveMultipleRequest
}

# Function to remove a record from Dataverse
function Remove-Record {
    param (
        [Parameter(Mandatory)] 
        [String]
        $setName,
        [Parameter(Mandatory)] 
        [Guid] 
        $id,
        [bool] 
        $strongConsistency = $false
    )
    $uri = $baseURI + $setName
    $uri = $uri + '(' + $id.Guid + ')'
    $deleteHeaders = $baseHeaders.Clone()
    if ($strongConsistency) {
        $deleteHeaders.Add('Consistency', 'Strong')
    }

    $DeleteRequest = @{
        Uri     = $uri
        Method  = 'Delete'
        Headers = $deleteHeaders
    }
    Invoke-ResilientRestMethod $DeleteRequest
}

#endregion Core functions

#region Metadata functions

# Function to get tables from Dataverse
function Get-Tables {
    param (
        [Parameter(Mandatory)] 
        [String] 
        $query
    )
    $uri = $baseURI + 'EntityDefinitions' + $query
    # Header for GET operations that have annotations
    $getHeaders = $baseHeaders.Clone()
    $getHeaders.Add('If-None-Match', $null)
    $getHeaders.Add('Consistency', 'Strong')
    $RetrieveMultipleRequest = @{
        Uri     = $uri
        Method  = 'Get'
        Headers = $getHeaders
    }
    Invoke-ResilientRestMethod $RetrieveMultipleRequest
}

# Function to create a new global option set
function New-GlobalOptionSet {
    param (
        [Parameter(Mandatory)] 
        [hashtable]
        $optionSet,
        [string] 
        $solutionUniqueName
    )

    $postHeaders = $baseHeaders.Clone()
    $postHeaders.Add('Content-Type', 'application/json')
    $postHeaders.Add('Consistency', 'Strong')
    if ($solutionUniqueName) {
        $postHeaders.Add('MSCRM.SolutionUniqueName', $solutionUniqueName)
    }
    
    $CreateRequest = @{
        Uri     = $baseURI + 'GlobalOptionSetDefinitions'
        Method  = 'Post'
        Headers = $postHeaders
        Body    = ConvertTo-Json $optionSet -Depth 10
    }
    
    $rh = Invoke-ResilientRestMethod -request $CreateRequest -returnHeader $true
    $url = $rh['OData-EntityId']
    $selectedString = Select-String -InputObject $url -Pattern '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
    return [System.Guid]::New($selectedString.Matches.Value.ToString())
}

# Function to get a global option set from Dataverse
function Get-GlobalOptionSet {
    param (
        [String] 
        $name,
        [guid] 
        $id,
        [string]
        $type,
        [String] 
        $query
    )

    $key = ''
    if ($id) {
        $key = "($id)"
    }
    elseif ($name) {
        $key = "(Name='$name')"
    }
    else {
        throw 'Either the name or the id of the global option set must be provided.'
    }

    $typeName = switch ($type) {
        'OptionSet' { '/Microsoft.Dynamics.CRM.OptionSetMetadata' }
        'Boolean' { '/Microsoft.Dynamics.CRM.BooleanOptionSetMetadata' }
        Default {
            ''
            # If the type isn't set the function will not enable expanding the options.
        }
    }

    $uri = $baseURI + 'GlobalOptionSetDefinitions' + $key + $typeName + $query
    $getHeaders = $baseHeaders.Clone()
    $getHeaders.Add('Consistency', 'Strong')
    $RetrieveRequest = @{
        Uri     = $uri
        Method  = 'Get'
        Headers = $getHeaders
    }

    try {
        Invoke-ResilientRestMethod $RetrieveRequest
    }
    catch [Microsoft.PowerShell.Commands.HttpResponseException] {
        Write-Host "HTTP Response Error: $_.Exception.Message "
       
        $statuscode = $_.Exception.Response.StatusCode
        # 404 errors only
        if ($statuscode -eq 'NotFound') {
            # Return $null if the global option set is not found
            return $null
        }
        else {
            throw $_
        }
    }
    catch {
        throw $_
    }
}

# Function to create a new table in Dataverse
function New-Table {
    param (
        [Parameter(Mandatory)] 
        [hashtable]
        $body,
        [String] 
        $solutionUniqueName
    )

    $postHeaders = $baseHeaders.Clone()
    $postHeaders.Add('Content-Type', 'application/json')
    $postHeaders.Add('Consistency', 'Strong')
    if ($solutionUniqueName -ne $null) {
        $postHeaders.Add('MSCRM.SolutionUniqueName', $solutionUniqueName)
    }
    
    $CreateRequest = @{
        Uri     = $baseURI + 'EntityDefinitions'
        Method  = 'Post'
        Headers = $postHeaders
        Body    = ConvertTo-Json $body -Depth 10
    }
    
    $rh = Invoke-ResilientRestMethod -request $CreateRequest -returnHeader $true
    $url = $rh['OData-EntityId']
    $selectedString = $url | Select-String -Pattern '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' -AllMatches | % { $_.Matches }
    return [System.Guid]::New($selectedString.Value.ToString())
}

# Function to create a new relationship in Dataverse
function New-Relationship {
    param (
        [Parameter(Mandatory)] 
        [hashtable]
        $relationship,
        [String] 
        $solutionUniqueName
    )

    $postHeaders = $baseHeaders.Clone()
    $postHeaders.Add('Content-Type', 'application/json')
    $postHeaders.Add('Consistency', 'Strong')
    if ($solutionUniqueName -ne $null) {
        $postHeaders.Add('MSCRM.SolutionUniqueName', $solutionUniqueName)
    }
    
    $CreateRequest = @{
        Uri     = $baseURI + 'RelationshipDefinitions'
        Method  = 'Post'
        Headers = $postHeaders
        Body    = ConvertTo-Json $relationship -Depth 10
    }
    
    $rh = Invoke-ResilientRestMethod -request $CreateRequest -returnHeader $true
    $url = $rh['OData-EntityId']
    $selectedString = $url | Select-String -Pattern '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' -AllMatches | % { $_.Matches }
    return [System.Guid]::New($selectedString.Value.ToString())
}

#endregion Metadata functions

# Variables to store created components
$recordsToDelete = @()
$createdOptionSets = @{}
$createdTables = @{}
$languageCode = 1033

# Connect to Dataverse using client secret
Connect-Dataverse -uri $OrganizationUrl -tenantId $TenantId -clientId $ClientId -clientSecret $ClientSecret

# Main execution within Dataverse Commands to ensure proper error handling
Invoke-DataverseCommands {
    
    # Parse ERD File
    function Parse-ErdFile {
        param($filePath)
        
        Write-Host "Parsing ERD file: $filePath" -ForegroundColor Cyan
        $content = Get-Content -Path $filePath -Raw
        
        $result = @{
            Tables        = @{}
            Enums         = @{}
            Relationships = @()
        }
        
        # Extract enums 
        $enumMatches = [regex]::Matches($content, "Enum\s+(\w+)\s+{([^}]*)}")
        foreach ($enumMatch in $enumMatches) {
            $enumName = $enumMatch.Groups[1].Value
            $enumContent = $enumMatch.Groups[2].Value
            
            $enumValues = @()
            $valueLines = $enumContent -split "\r?\n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -and $_ -notmatch "^\s*//" }
            
            foreach ($line in $valueLines) {
                if (-not [string]::IsNullOrWhiteSpace($line)) {
                    # Remove trailing commas and quotes
                    $cleanValue = $line -replace '[,\''""]', '' -replace '\s+', ''
                    if ($cleanValue) {
                        $enumValues += $cleanValue
                    }
                }
            }
            
            $result.Enums[$enumName] = $enumValues
        }
        
        # Extract tables and their fields
        $tableMatches = [regex]::Matches($content, "Table\s+(\w+)\s+{([^}]*)}")
        foreach ($tableMatch in $tableMatches) {
            $tableName = $tableMatch.Groups[1].Value
            $tableContent = $tableMatch.Groups[2].Value
            
            $fields = @{}
            $fieldLines = $tableContent -split "\r?\n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -and $_ -notmatch "^\s*//" }
            
            foreach ($line in $fieldLines) {
                if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith("//")) {
                    continue
                }
                
                # Parse field definition 
                $fieldParts = $line -split "\s+", 3
                if ($fieldParts.Count -ge 2) {
                    $fieldName = $fieldParts[0]
                    $fieldType = $fieldParts[1]
                    
                    # Check if this is a primary key
                    $isPk = $line -match "\[pk\]"
                    
                    # Check for not null
                    $isRequired = $line -match "\[not null\]"
                    
                    # Check if it's an enum
                    $isEnum = $result.Enums.ContainsKey($fieldType)
                    $enumValues = @()
                    
                    if ($isEnum) {
                        $enumValues = $result.Enums[$fieldType]
                    }
                    
                    $fields[$fieldName] = @{
                        Name         = $fieldName
                        Type         = $fieldType
                        IsPrimaryKey = $isPk
                        IsEnum       = $isEnum
                        EnumName     = if ($isEnum) { $fieldType } else { $null }
                        EnumValues   = $enumValues
                        Required     = $isRequired
                    }
                }
            }
            
            $result.Tables[$tableName] = @{
                Name        = $tableName
                Fields      = $fields
                LogicalName = "${PublisherPrefix}$($tableName.ToLower())"
            }
        }
        
        # Extract relationships
        $relationshipMatches = [regex]::Matches($content, "Ref:\s+""([^""]+)""\.""([^""]+)""\s+([<>])\s+""([^""]+)""\.""([^""]+)""")
        foreach ($relMatch in $relationshipMatches) {
            $relationship = @{
                FromTable = $relMatch.Groups[4].Value
                FromField = $relMatch.Groups[5].Value
                Direction = $relMatch.Groups[3].Value
                ToTable   = $relMatch.Groups[1].Value
                ToField   = $relMatch.Groups[2].Value
            }
            $result.Relationships += $relationship
        }
        
        # Process relationships to identify lookups
        foreach ($relationship in $result.Relationships) {
            if ($relationship.Direction -eq "<") {
                # This is a lookup relationship (many-to-one)
                # The "many" side has the lookup to the "one" side
                $fromTable = $relationship.FromTable
                $toTable = $relationship.ToTable
                $toField = $relationship.ToField
                
                # Add lookup field info to the "many" side table
                if ($result.Tables.ContainsKey($toTable)) {
                    $fieldName = "${fromTable}id"
                    if (-not $result.Tables[$toTable].Fields.ContainsKey($fieldName)) {
                        $result.Tables[$toTable].Fields[$fieldName] = @{
                            Name          = $fieldName
                            Type          = "Lookup"
                            LookupTo      = $fromTable
                            LookupToField = $relationship.FromField
                            IsLookup      = $true
                            Required      = $false
                        }
                    }
                }
            }
        }
        
        Write-Host "Successfully parsed ERD file:" -ForegroundColor Green
        Write-Host "  - $($result.Tables.Count) tables" -ForegroundColor Green
        Write-Host "  - $($result.Enums.Count) enum definitions" -ForegroundColor Green
        Write-Host "  - $($result.Relationships.Count) relationships" -ForegroundColor Green
        return $result
    }
    
    # Create Global Option Sets
    function Create-GlobalOptionSets {
        param($erdData)
        
        $globalOptionSets = @{}
        
        foreach ($enumName in $erdData.Enums.Keys) {
            Write-Host "Processing global option set: $enumName" -ForegroundColor Cyan
            $schemaName = "${PublisherPrefix}$enumName"
            $displayName = $enumName #-replace "([a-z])([A-Z])", '$1 $2'
            $enumValues = $erdData.Enums[$enumName]
            
            # Check if option set already exists
            $existingOptionSet = Get-GlobalOptionSet -name $schemaName -type 'OptionSet'
            
            if ($null -ne $existingOptionSet) {
                Write-Host "  Option set $schemaName already exists, skipping..." -ForegroundColor Yellow
                $globalOptionSets[$enumName] = @{
                    SchemaName  = $schemaName
                    DisplayName = $displayName
                    MetadataId  = $existingOptionSet.MetadataId
                }
                continue
            }
            
            if ($WhatIf) {
                Write-Host "  WhatIf: Would create option set $schemaName with $($enumValues.Count) values" -ForegroundColor Magenta
                $globalOptionSets[$enumName] = @{
                    SchemaName  = $schemaName
                    DisplayName = $displayName
                    MetadataId  = "00000000-0000-0000-0000-000000000000" # Dummy ID for WhatIf
                }
                continue
            }
            
            # Create the option set definition
            $optionSetData = @{
                '@odata.type' = 'Microsoft.Dynamics.CRM.OptionSetMetadata'
                Name          = $schemaName
                DisplayName   = @{
                    LocalizedLabels = @(
                        @{
                            Label        = $displayName
                            LanguageCode = $languageCode
                        }
                    )
                }
                Description   = @{
                    LocalizedLabels = @(
                        @{
                            Label        = "$displayName option set"
                            LanguageCode = $languageCode
                        }
                    )
                }
                IsGlobal      = $true
                Options       = @()
            }
            
            # Add options to the option set
            for ($i = 0; $i -lt $enumValues.Count; $i++) {
                $value = $enumValues[$i]
                $optionValue = @{
                    Label = @{
                        LocalizedLabels = @(
                            @{
                                Label        = $value
                                LanguageCode = $languageCode
                            }
                        )
                    }
                    Value = 100000000 + $i  # Base value plus index
                }
                $optionSetData.Options += $optionValue
            }
            
            try {
                # Create the global option set
                $optionSetId = New-GlobalOptionSet -optionSet $optionSetData -solutionUniqueName $SolutionName
                
                Write-Host "  Created global option set $schemaName with ID: $optionSetId" -ForegroundColor Green
                
                $globalOptionSets[$enumName] = @{
                    SchemaName  = $schemaName
                    DisplayName = $displayName
                    MetadataId  = $optionSetId
                }
                
                # Add to records to delete if needed
                # $optionSetRecordToDelete = @{
                #     'description' = "Global Option Set '$schemaName'"
                #     'setName' = 'GlobalOptionSetDefinitions'
                #     'id' = $optionSetId
                # }
                # $recordsToDelete += $optionSetRecordToDelete
                $isDuplicate = $recordsToDelete | Where-Object { 
                    $_.description -eq $optionSetRecordToDelete.description -and 
                    $_.setName -eq $optionSetRecordToDelete.setName -and 
                    $_.id -eq $optionSetRecordToDelete.id 
                }
                
                if (-not $isDuplicate) {
                    $recordsToDelete += $optionSetRecordToDelete
                }
            }
            catch {
                Write-Host "  Error creating global option set $schemaName" -ForegroundColor Red
                Write-Host "  $($_.Exception.Message)" -ForegroundColor Red
            }
        }
        
        return $globalOptionSets
    }
    
    # Create Tables
    function Create-DataverseTables {
        param(
            $erdData,
            $globalOptionSets
        )
        
        $createdTables = @{}
        
        foreach ($tableName in $erdData.Tables.Keys) {
            Write-Host "Processing table: $tableName" -ForegroundColor Cyan
            $table = $erdData.Tables[$tableName]
            
            # Skip tables that extend existing Dynamics tables (marked with @extends)
            if ($tableName -match "Dynamics_") {
                Write-Host "  Skipping Dynamics core table: $tableName" -ForegroundColor Yellow
                continue
            }
            
            $schemaName = $table.LogicalName
            $displayName = $tableName #-replace "([a-z])([A-Z])", '$1 $2'
            $pluralName = if ($displayName -match "y$") {
                $displayName -replace "y$", "ies"
            }
            else {
                $displayName + "s"
            }
            
            # Check if table already exists
            $tableQuery = "?`$filter=SchemaName eq '$schemaName'"
            $tableQueryResults = (Get-Tables -query $tableQuery).value
            
            if ($tableQueryResults.Count -gt 0) {
                Write-Host "  Table $schemaName already exists, skipping creation..." -ForegroundColor Yellow
                $createdTables[$tableName] = @{
                    SchemaName  = $schemaName
                    DisplayName = $displayName
                    MetadataId  = $tableQueryResults[0].MetadataId
                }
                continue
            }
            
            if ($WhatIf) {
                Write-Host "  WhatIf: Would create table $schemaName" -ForegroundColor Magenta
                $createdTables[$tableName] = @{
                    SchemaName  = $schemaName
                    DisplayName = $displayName
                    MetadataId  = "00000000-0000-0000-0000-000000000000" # Dummy ID for WhatIf
                }
                continue
            }
            
            # Create the table definition
            $tableDefinition = @{
                '@odata.type'         = "Microsoft.Dynamics.CRM.EntityMetadata"
                SchemaName            = $schemaName
                DisplayName           = @{
                    '@odata.type'   = 'Microsoft.Dynamics.CRM.Label'
                    LocalizedLabels = @(
                        @{
                            '@odata.type' = 'Microsoft.Dynamics.CRM.LocalizedLabel'
                            Label         = $displayName
                            LanguageCode  = $languageCode
                        }
                    )
                }
                DisplayCollectionName = @{
                    '@odata.type'   = 'Microsoft.Dynamics.CRM.Label'
                    LocalizedLabels = @(
                        @{
                            '@odata.type' = 'Microsoft.Dynamics.CRM.LocalizedLabel'
                            Label         = $pluralName
                            LanguageCode  = $languageCode
                        }
                    )
                }
                Description           = @{
                    '@odata.type'   = 'Microsoft.Dynamics.CRM.Label'
                    LocalizedLabels = @(
                        @{
                            '@odata.type' = 'Microsoft.Dynamics.CRM.LocalizedLabel'
                            Label         = "Table for $displayName data"
                            LanguageCode  = $languageCode
                        }
                    )
                }
                HasActivities         = $false
                HasNotes              = $false
                OwnershipType         = 'UserOwned'
                Attributes            = @()
            }
            
            # Find primary name field or create one
            $primaryNameField = $null
            foreach ($fieldName in $table.Fields.Keys) {
                $field = $table.Fields[$fieldName]
                if ($field.IsPrimaryKey) {
                    $primaryNameField = $fieldName
                    $primaryKeyField = $fieldName
                }
                # Look for Name field or a string field
                # if (($fieldName -match "Name" -or $fieldName -match "Title") -and $field.Type -match "String") {
                #     $primaryNameField = $fieldName
                #     break
                # } elseif ($field.IsPrimaryKey) {
                #     $primaryKeyField = $fieldName
                # }
            }
            
            Write-Host "PrimaryKeyField Name: $primaryKeyField ; $primaryNameField"

            # If no primary name field found, create one
            # if ($null -eq $primaryNameField) {
            $primaryNameField = $primaryNameField.ToLower()
            $nameAttribute = @{
                '@odata.type' = 'Microsoft.Dynamics.CRM.StringAttributeMetadata'
                SchemaName    = ${PublisherPrefix} + $($primaryNameField.ToLower())
                IsPrimaryName = $true
                RequiredLevel = @{
                    Value = 'ApplicationRequired'
                }
                DisplayName   = @{
                    '@odata.type'   = 'Microsoft.Dynamics.CRM.Label'
                    LocalizedLabels = @(
                        @{
                            '@odata.type' = 'Microsoft.Dynamics.CRM.LocalizedLabel'
                            Label         = $($primaryNameField.ToLower())
                            LanguageCode  = $languageCode
                        }
                    )
                }
                Description   = @{
                    '@odata.type'   = 'Microsoft.Dynamics.CRM.Label'
                    LocalizedLabels = @(
                        @{
                            '@odata.type' = 'Microsoft.Dynamics.CRM.LocalizedLabel'
                            Label         = 'The name of the record'
                            LanguageCode  = $languageCode
                        }
                    )
                }
                MaxLength     = 100
            }
            $tableDefinition.Attributes += $nameAttribute
            #}
            
            
            Write-Host "field name used: ${PublisherPrefix}$($primaryNameField.ToLower())"
            $tableDefinition.PrimaryNameAttribute = "${PublisherPrefix}$($primaryNameField.ToLower())"
            
            # Process fields
            foreach ($fieldName in $table.Fields.Keys) {
                $field = $table.Fields[$fieldName]
                $fieldSchemaName = "${PublisherPrefix}$($fieldName.ToLower())"
                $fieldDisplayName = $fieldName #-replace "([a-z])([A-Z])", '$1 $2'
                
                # Skip primary key fields (will be auto-generated)
                if ($field.IsPrimaryKey -or $field.Type -eq "GUID" ) {
                    continue
                }
                
                
                # Process enum fields (references to global option sets)
                if ($field.IsEnum) {
                    $enumName = $field.EnumName
                    Write-Host "enum: $enumName"
                    if ($globalOptionSets.ContainsKey($enumName)) {
                        $optionSetAttribute = @{
                            '@odata.type'                = 'Microsoft.Dynamics.CRM.PicklistAttributeMetadata'
                            SchemaName                   = $fieldSchemaName
                            RequiredLevel                = @{
                                Value = if ($field.Required) { 'ApplicationRequired' } else { 'None' }
                            }
                            DisplayName                  = @{
                                '@odata.type'   = 'Microsoft.Dynamics.CRM.Label'
                                LocalizedLabels = @(
                                    @{
                                        '@odata.type' = 'Microsoft.Dynamics.CRM.LocalizedLabel'
                                        Label         = $fieldDisplayName
                                        LanguageCode  = $languageCode
                                    }
                                )
                            }
                            Description                  = @{
                                '@odata.type'   = 'Microsoft.Dynamics.CRM.Label'
                                LocalizedLabels = @(
                                    @{
                                        '@odata.type' = 'Microsoft.Dynamics.CRM.LocalizedLabel'
                                        Label         = "$fieldDisplayName field"
                                        LanguageCode  = $languageCode
                                    }
                                )
                            }
                            'GlobalOptionSet@odata.bind' = "/GlobalOptionSetDefinitions($($globalOptionSets[$enumName].MetadataId))"
                        }
                        
                        $tableDefinition.Attributes += $optionSetAttribute
                    }
                }
                # Process lookup fields
                elseif ($field.Type -eq "Lookup") {
                    # We'll handle lookups after all tables are created
                    continue
                }
                #
                #establish the people look up field here....
                elseif ($field.Type -eq "Person") {
                    #this is a special case and gets done after lookups
                    continue
                }
                # Process regular fields
                else {
                    # Map types to Dataverse types
                    $dataverseType = switch -Regex ($field.Type.ToLower()) {
                        "guid" { 
                            @{Type = "UniqueIdentifier"; ODataType = "Microsoft.Dynamics.CRM.UniqueIdentifierAttributeMetadata" } 
                        }
                        "string" { 
                            @{Type = "String"; ODataType = "Microsoft.Dynamics.CRM.StringAttributeMetadata"; MaxLength = 255 } 
                        }
                        "text|varchar" { 
                            @{Type = "Memo"; ODataType = "Microsoft.Dynamics.CRM.MemoAttributeMetadata"; MaxLength = 2000; Format = "TextArea" } 
                        }
                        "integer|int" { 
                            @{Type = "Integer"; ODataType = "Microsoft.Dynamics.CRM.IntegerAttributeMetadata" } 
                        }
                        "decimal|money" { 
                            @{Type = "Decimal"; ODataType = "Microsoft.Dynamics.CRM.DecimalAttributeMetadata"; Precision = 2 } 
                        }
                        "boolean|bool" { 
                            @{Type = "Boolean"; ODataType = "Microsoft.Dynamics.CRM.BooleanAttributeMetadata" } 
                        }
                        "datetime|date" { 
                            @{Type = "DateTime"; ODataType = "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata" } 
                        }
                        default { 
                            @{Type = "String"; ODataType = "Microsoft.Dynamics.CRM.StringAttributeMetadata"; MaxLength = 255 } 
                        }
                    }
                    
                    $attribute = @{
                        '@odata.type' = $dataverseType.ODataType
                        SchemaName    = $fieldSchemaName
                        RequiredLevel = @{
                            Value = if ($field.Required) { 'ApplicationRequired' } else { 'None' }
                        }
                        DisplayName   = @{
                            '@odata.type'   = 'Microsoft.Dynamics.CRM.Label'
                            LocalizedLabels = @(
                                @{
                                    '@odata.type' = 'Microsoft.Dynamics.CRM.LocalizedLabel'
                                    Label         = $fieldDisplayName
                                    LanguageCode  = $languageCode
                                }
                            )
                        }
                        Description   = @{
                            '@odata.type'   = 'Microsoft.Dynamics.CRM.Label'
                            LocalizedLabels = @(
                                @{
                                    '@odata.type' = 'Microsoft.Dynamics.CRM.LocalizedLabel'
                                    Label         = "$fieldDisplayName field"
                                    LanguageCode  = $languageCode
                                }
                            )
                        }
                    }
                    
                    # Add type-specific properties
                    if ($dataverseType.Type -eq "String") {
                        $attribute.MaxLength = $dataverseType.MaxLength
                    }
                    elseif ($dataverseType.Type -eq "Memo") {
                        $attribute.MaxLength = $dataverseType.MaxLength
                        $attribute.Format = $dataverseType.Format
                    }
                    elseif ($dataverseType.Type -eq "Decimal") {
                        $attribute.Precision = $dataverseType.Precision
                    }
                    elseif ($dataverseType.Type -eq "DateTime") {
                        $attribute.DateTimeBehavior = @{
                            Value = "UserLocal"
                        }
                        $attribute.Format = "DateAndTime"
                    }
                    elseif ($dataverseType.Type -eq "Boolean") {
                        $attribute.DefaultValue = $false
                        $attribute.OptionSet = @{
                            TrueOption    = @{
                                Value = 1
                                Label = @{
                                    "@odata.type"   = "Microsoft.Dynamics.CRM.Label"
                                    LocalizedLabels = @(
                                        @{
                                            "@odata.type" = "Microsoft.Dynamics.CRM.LocalizedLabel"
                                            Label         = "True"
                                            LanguageCode  = 1033
                                            IsManaged     = $false
                                        }
                                    )
                                }
                            }
                            FalseOption   = @{
                                Value = 0
                                Label = @{
                                    "@odata.type"   = "Microsoft.Dynamics.CRM.Label"
                                    LocalizedLabels = @(
                                        @{
                                            "@odata.type" = "Microsoft.Dynamics.CRM.LocalizedLabel"
                                            Label         = "False"
                                            LanguageCode  = 1033
                                            IsManaged     = $false
                                        }
                                    )
                                }
                            }
                            OptionSetType = "Boolean"
                        }
                    }
                    
                    # Set IsPrimaryName if this is the primary name field
                    if ($fieldName -eq $primaryNameField) {
                        $attribute.IsPrimaryName = $true
                    }
                    
                    $tableDefinition.Attributes += $attribute
                }
            }
            
            try {
                # Create the table
                $tableId = New-Table -body $tableDefinition -solutionUniqueName $SolutionName
                
                Write-Host "  Created table $schemaName with ID: $tableId" -ForegroundColor Green
                
                $createdTables[$tableName] = @{
                    SchemaName  = $schemaName
                    DisplayName = $displayName
                    MetadataId  = $tableId
                    LogicalName = $schemaName
                }
                
                # Add to records to delete if needed
                $tableRecordToDelete = @{
                    'description' = "Table '$schemaName'"
                    'setName'     = 'EntityDefinitions'
                    'id'          = $tableId
                }
                $recordsToDelete += $tableRecordToDelete
            }
            catch {
                Write-Host "  Error creating table $schemaName" -ForegroundColor Red
                Write-Host "  $($_.Exception.Message)" -ForegroundColor Red
            }
        }
        
        return $createdTables
    }
    
    # Create Lookup Relationships
    function Create-LookupRelationships {
        param(
            $erdData,
            $createdTables
        )
        
        $createdRelationships = @()
        
        foreach ($relationship in $erdData.Relationships) {
            $fromTable = $relationship.FromTable
            $fromField = $relationship.FromField
            Write-Host "fromField $fromField.ToLower()"
            $direction = $relationship.Direction
            $toTable = $relationship.ToTable
            $toField = $relationship.ToField
            Write-Host "toField $toField.ToLower()"
            
            # Skip relationships involving Dynamics core tables
            #we are going to add the dynamics tables regardless.
            #if ($fromTable -match "Dynamics_" -or $toTable -match "Dynamics_") {
             #   Write-Host "  Skipping relationship involving Dynamics core table: $fromTable -> $toTable" -ForegroundColor Yellow
              #  continue
            #}
            
            # Handle based on direction
            if ($direction -eq "<") {
                # This is a many-to-one (lookup) relationship
                # The "many" side has the lookup to the "one" side
                
                # Check if both tables were created
                #assume the target tables exist -- possibly run as 2 separate scrips
                #if (-not $createdTables.ContainsKey($fromTable) -or -not $createdTables.ContainsKey($toTable)) {
                    #Write-Host "  Skipping relationship, one or both tables not created: $fromTable -> $toTable" -ForegroundColor Yellow
                    #continue
                #}
                Write-Host "   relationship: $fromTable -> $toTable" -ForegroundColor Yellow
                #$fromTableSchema = $createdTables[$fromTable].SchemaName
                #$toTableSchema = $createdTables[$toTable].SchemaName
                #note if its a coretable do not prefix - so skip for dynamics_
                if ($fromTable -match "Dynamics_") {
                    $fromTableLogicalName = ${fromTable}.Replace("Dynamics_","").ToLower() #$createdTables[$fromTable].LogicalName
                }else{
                    $fromTableLogicalName = ${PublisherPrefix}.ToLower() + ${fromTable}.ToLower() #$createdTables[$fromTable].LogicalName

                }

                if ($toTable -match "Dynamics_") {
                    $toTableLogicalName = ${toTable}.Replace("Dynamics_","").ToLower()#$createdTables[$toTable].LogicalName
                }
                else{
                    $toTableLogicalName = ${PublisherPrefix}.ToLower() + ${toTable}.ToLower()#$createdTables[$toTable].LogicalName
                }
                # Create relationship name
                $relationshipName = "${PublisherPrefix}${fromTable}_${toTable}"
                Write-Host "  Creating lookup relationship: $relationshipName" -ForegroundColor Cyan
                
                if ($WhatIf) {
                    Write-Host "    WhatIf: Would create lookup relationship $relationshipName" -ForegroundColor Magenta
                    continue
                }
                
                # Create lookup field on the "many" side
                if ($toTable -match "Dynamics_") {
                    $lookupFieldName = ${toField}.ToLower()
                }else{
                    $lookupFieldName = ${PublisherPrefix}.ToLower() + ${toField}.ToLower()
                }
                
                $lookupDisplayName = "$toTable"
                Write-Host "logicalNamesTest RDP: ${PublisherPrefix}$fromTable to ${PublisherPrefix}$toTable fromField ${fromField.ToLower()}"
                Write-Host "logicalNames:$fromTableLogicalName ; $toTableLogicalName"
                # Create the relationship
                $relationshipData = @{
                    '@odata.type'               = 'Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata'
                    SchemaName                  = $relationshipName
                    ReferencedAttribute = ($toTable -match "Dynamics_") ? $toField.ToLower() : "${PublisherPrefix}$($toField.ToLower())"
                    #ReferencedAttribute         = ${PublisherPrefix} + $toField.ToLower()
                    ReferencedEntity            = $toTableLogicalName
                    ReferencingEntity           = $fromTableLogicalName 
                    Lookup                      = @{
                        SchemaName  = $lookupFieldName + "_" + $relationshipName
                        DisplayName = @{
                            LocalizedLabels = @(
                                @{
                                    Label        = $lookupDisplayName
                                    LanguageCode = $languageCode
                                }
                            )
                        }
                        Description = @{
                            LocalizedLabels = @(
                                @{
                                    Label        = "Reference to $toTable"
                                    LanguageCode = $languageCode
                                }
                            )
                        }
                    }
                    AssociatedMenuConfiguration = @{
                        Behavior = 'UseLabel'
                        Group    = 'Details'
                        Label    = @{
                            LocalizedLabels = @(
                                @{
                                    Label        = $toTable
                                    LanguageCode = $languageCode
                                }
                            )
                        }
                        Order    = 10000
                    }
                    CascadeConfiguration        = @{
                        Assign     = 'NoCascade'
                        Share      = 'NoCascade'
                        Unshare    = 'NoCascade'
                        RollupView = 'NoCascade'
                        Reparent   = 'NoCascade'
                        Delete     = 'RemoveLink'
                        Merge      = 'NoCascade'
                    }
                }

                # Define the lookup column metadata with an explicit reference to the table
                #test
                try {
                    # Create relationship
                    Write-Host "Relationship Data: $relationshipData" 
                    $relationshipId = New-Relationship -relationship $relationshipData -solutionUniqueName $SolutionName
                    Write-Host "    Created relationship $relationshipName with ID: $relationshipId" -ForegroundColor Green
                    
                    $createdRelationships += @{
                        Name = $relationshipName
                        Id   = $relationshipId
                    }
                    
                    # Add to records to delete if needed
                    $relationshipRecordToDelete = @{
                        'description' = "Relationship '$relationshipName'"
                        'setName'     = 'RelationshipDefinitions'
                        'id'          = $relationshipId
                    }
                    $recordsToDelete += $relationshipRecordToDelete
                }
                catch {
                    Write-Host "    Error creating relationship $relationshipName" -ForegroundColor Red
                    Write-Host "    $($_.Exception.Message)" -ForegroundColor Red
                }
            }
        }

        

        return $createdRelationships
    }
    
    function Create-UserLookUps{
        param(
            $erdData
            
        )

        foreach ($tableName in $erdData.Tables.Keys) {
            Write-Host "Processing table: $tableName" -ForegroundColor Cyan
            $table = $erdData.Tables[$tableName]
            
            # Skip tables that extend existing Dynamics tables (marked with @extends)
            if ($tableName -match "Dynamics_") {
                Write-Host "  Skipping Dynamics core table: $tableName" -ForegroundColor Yellow
                continue
            }
            
            $schemaName = $table.LogicalName
            $displayName = $tableName #-replace "([a-z])([A-Z])", '$1 $2'
            $pluralName = if ($displayName -match "y$") {
                $displayName -replace "y$", "ies"
            }
            else {
                $displayName + "s"
            }
			
			foreach ($fieldName in $table.Fields.Keys) {
                $field = $table.Fields[$fieldName]
                $fieldSchemaName = "${PublisherPrefix}$($fieldName.ToLower())"
                $fieldDisplayName = $fieldName #-replace "([a-z])([A-Z])", '$1 $2'
				if ($field.Type -eq "Person") {
                    Add-UserLookupField -tableLogicalName $schemaName -userFieldName $fieldSchemaName  -displayName $fieldDisplayName -description $fieldDisplayName -solutionUniqueName $SolutionName
    
                }
				else{
					continue
				}
			}
        }

    }



    # Create a user lookup field
function Add-UserLookupField {
    param (
        [Parameter(Mandatory)]
        [string]$tableLogicalName,
        [Parameter(Mandatory)]
        [string]$userFieldName,
        [Parameter(Mandatory)]
        [string]$displayName,
        [string]$description = "User lookup field",
        [string]$solutionUniqueName
    )
    
    $lookupSchemaName = "${PublisherPrefix}${userFieldName}"
    
    # Create the relationship data for a lookup to systemuser
    $userLookupData = @{
        '@odata.type' = 'Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata'
        SchemaName = "${PublisherPrefix}${tableLogicalName}_systemuser_${userFieldName}"
        ReferencedEntity = "systemuser"
        ReferencedAttribute = "systemuserid"
        ReferencingEntity = $tableLogicalName
        Lookup = @{
            SchemaName = $lookupSchemaName
            DisplayName = @{
                LocalizedLabels = @(
                    @{
                        Label = $displayName
                        LanguageCode = $languageCode
                    }
                )
            }
            Description = @{
                LocalizedLabels = @(
                    @{
                        Label = $description
                        LanguageCode = $languageCode
                    }
                )
            }
        }
        AssociatedMenuConfiguration = @{
            Behavior = 'UseLabel'
            Group = 'Details'
            Label = @{
                LocalizedLabels = @(
                    @{
                        Label = $tableLogicalName
                        LanguageCode = $languageCode
                    }
                )
            }
            Order = 10000
        }
        CascadeConfiguration = @{
            Assign = 'NoCascade'
            Share = 'NoCascade'
            Unshare = 'NoCascade'
            RollupView = 'NoCascade'
            Reparent = 'NoCascade'
            Delete = 'RemoveLink'
            Merge = 'NoCascade'
        }
    }
    
    try {
        # Create the lookup relationship to systemuser
        $relationshipId = New-Relationship -relationship $userLookupData -solutionUniqueName $solutionUniqueName
        Write-Host "  Created user lookup field $lookupSchemaName with relationship ID: $relationshipId" -ForegroundColor Green
        
        return @{
            SchemaName = $lookupSchemaName
            RelationshipId = $relationshipId
        }
    }
    catch {
        Write-Host "  Error creating user lookup field $lookupSchemaName" -ForegroundColor Red
        Write-Host "  $($_.Exception.Message)" -ForegroundColor Red
        return $null
    }
}

    function Generate-Summary {
        param(
            $erdData,
            $globalOptionSets,
            $createdTables,
            $createdRelationships
        )
        
        Write-Host "`n----------------------------------------" -ForegroundColor Green
        Write-Host "         Execution Summary" -ForegroundColor Green
        Write-Host "----------------------------------------" -ForegroundColor Green
        
        # Option Sets Summary
        Write-Host "`nGlobal Option Sets:" -ForegroundColor Cyan
        foreach ($enumName in $erdData.Enums.Keys) {
            $status = if ($globalOptionSets.ContainsKey($enumName)) { "Created" } else { "Failed" }
            $statusColor = if ($status -eq "Created") { "Green" } else { "Red" }
            Write-Host "  - $enumName : " -NoNewline
            Write-Host $status -ForegroundColor $statusColor
            if ($status -eq "Created") {
                Write-Host "      Schema Name: $($globalOptionSets[$enumName]['SchemaName'])" -ForegroundColor Gray
                Write-Host "      Values: $($erdData.Enums[$enumName] -join ', ')" -ForegroundColor Gray
            }
        }
        
        # Tables Summary
        Write-Host "`nDataverse Tables:" -ForegroundColor Cyan
        foreach ($tableName in $erdData.Tables.Keys) {
            # Skip Dynamics tables
            if ($tableName -match "Dynamics_") {
                Write-Host "  - $tableName : " -NoNewline
                Write-Host "Skipped (Core Table)" -ForegroundColor Yellow
                continue
            }
            
            $status = if ($createdTables.ContainsKey($tableName)) { "Created" } else { "Failed" }
            $statusColor = if ($status -eq "Created") { "Green" } else { "Red" }
            Write-Host "  - $tableName : " -NoNewline
            Write-Host $status -ForegroundColor $statusColor
            
            if ($status -eq "Created") {
                Write-Host "      Schema Name: $($createdTables[$tableName]['SchemaName'])" -ForegroundColor Gray
                
                # Count attributes
                $attributeCount = $erdData.Tables[$tableName].Fields.Count
                Write-Host "      Attributes: $attributeCount" -ForegroundColor Gray
                
                # Show fields with enum references
                $enumFields = $erdData.Tables[$tableName].Fields.GetEnumerator() | Where-Object { $_.Value.IsEnum }
                if ($enumFields.Count -gt 0) {
                    Write-Host "      Option Set Fields:" -ForegroundColor Gray
                    foreach ($enumField in $enumFields) {
                        Write-Host "        - $($enumField.Key) : $($enumField.Value.EnumName)" -ForegroundColor Gray
                    }
                }
                
                # Show lookup fields
                $lookupFields = $erdData.Tables[$tableName].Fields.GetEnumerator() | Where-Object { $_.Value.IsLookup }
                if ($lookupFields.Count -gt 0) {
                    Write-Host "      Lookup Fields:" -ForegroundColor Gray
                    foreach ($lookupField in $lookupFields) {
                        Write-Host "        - $($lookupField.Key) : References $($lookupField.Value.LookupTo)" -ForegroundColor Gray
                    }
                }
            }
        }
        
        # Relationships Summary
        Write-Host "`nRelationships:" -ForegroundColor Cyan
        if ($createdRelationships.Count -gt 0) {
            foreach ($relationship in $createdRelationships) {
                Write-Host "  - $($relationship.Name) : Created" -ForegroundColor Green
            }
        }
        else {
            Write-Host "  No relationships created" -ForegroundColor Yellow
        }
        
        # Summary statistics
        Write-Host "`n----------------------------------------" -ForegroundColor Green
        Write-Host "           Summary Counts" -ForegroundColor Green  
        Write-Host "----------------------------------------" -ForegroundColor Green
        Write-Host "ERD Definitions:" -ForegroundColor Cyan
        $nonDynamicsTables = ($erdData.Tables.Keys | Where-Object { $_ -notmatch "Dynamics_" }).Count
        Write-Host "  - Tables: $nonDynamicsTables" -ForegroundColor White
        Write-Host "  - Option Sets: $($erdData.Enums.Count)" -ForegroundColor White
        Write-Host "  - Relationships: $($erdData.Relationships.Count)" -ForegroundColor White
        Write-Host "`nCreated in Dataverse:" -ForegroundColor Cyan
        Write-Host "  - Tables: $($createdTables.Count) / $nonDynamicsTables" -ForegroundColor White
        Write-Host "  - Option Sets: $($globalOptionSets.Count) / $($erdData.Enums.Count)" -ForegroundColor White
        Write-Host "  - Relationships: $($createdRelationships.Count) / $($erdData.Relationships.Count)" -ForegroundColor White
    }
    
    # Main script execution
    try {
        # Parse the ERD file
        $erdData = Parse-ErdFile -filePath $ErdFilePath
        
        # Create global option sets for enums
        Write-Host "`nCreating Global Option Sets..." -ForegroundColor Cyan
       $globalOptionSets = Create-GlobalOptionSets -erdData $erdData
        
        # Create tables
        Write-Host "`nCreating Tables..." -ForegroundColor Cyan
       $createdTables = Create-DataverseTables -erdData $erdData -globalOptionSets $globalOptionSets
        
        # Create relationships
        Write-Host "`nCreating Relationships..." -ForegroundColor Cyan
        $createdRelationships = Create-LookupRelationships -erdData $erdData -createdTables $createdTables
        
        # Create Person type lookup fields
        Write-Host "`nCreating Person type Relationships..." -ForegroundColor Cyan
        Create-UserLookUps -erdData $erdData
        
        #Generate summary
        Generate-Summary -erdData $erdData -globalOptionSets $globalOptionSets -createdTables $createdTables -createdRelationships $createdRelationships
    }
    catch {
        Write-Host "`nAn error occurred during conversion:" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        
        if ($_.ScriptStackTrace) {
            Write-Host "Stack Trace: $($_.ScriptStackTrace)" -ForegroundColor Yellow
        }
    }
}