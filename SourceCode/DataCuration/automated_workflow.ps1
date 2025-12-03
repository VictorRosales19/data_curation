<#
PowerShell wrapper to run the data curation pipeline.

Important notes:
    Please activate a virtual environment before running this script
    Please set the environment variable US_CENSUS_API_KEY or skip demographics data
        To get a US_CENSUS_API_KEY please visit https://api.census.gov/data/key_signup.html

Usage examples:
  # Run full pipeline (default dates)
  .\automated_workflow.ps1

  # Run full pipeline but skip demographics
  .\automated_workflow.ps1 -SkipDemographics

  # Run only feature engineering for 2020-01
  .\automated_workflow.ps1 -FeaturesYear 2020 -FeaturesMonth 1 -OnlyFeatures
#>
param(
    [string]$RawData = "./RawData",
    [string]$TempData = "./TempData",
    [string]$CuratedData = "./CuratedData",
    [string]$StartDate = "2016-01-01",
    [string]$EndDate = "2025-10-01",
    [switch]$NoUnzip,
    [switch]$SkipDemographics,
    [switch]$SkipWeather,
    [switch]$FeaturesAll,
    [int]$FeaturesYear,
    [int]$FeaturesMonth,
    [switch]$OnlyFeatures,
    [int]$Sleep = 70
)

$py = "python"
$module = "-m SourceCode.DataCuration.run_pipeline"

$pyArgs = @("--raw-data", $RawData, "--temp-data", $TempData, "--curated-data", $CuratedData, "--start-date", $StartDate, "--end-date", $EndDate, "--sleep", $Sleep)
if ($NoUnzip) { $pyArgs += "--no-unzip" }
if ($SkipDemographics) { $pyArgs += "--skip-demographics" }
if ($SkipWeather) { $pyArgs += "--skip-weather" }
if ($FeaturesAll) { $pyArgs += "--features-all" }
if ($FeaturesYear) { $pyArgs += "--features-year"; $pyArgs += $FeaturesYear }
if ($FeaturesMonth) { $pyArgs += "--features-month"; $pyArgs += $FeaturesMonth }
if ($OnlyFeatures) {
  # when OnlyFeatures is provided, call run_pipeline with only feature args
  $pyArgs = @("--raw-data", $RawData, "--temp-data", $TempData, "--curated-data", $CuratedData, "--features-all")
}

# Build the final argument list and call Start-Process. Building the array first
# avoids parser issues when concatenating arrays inline.
$argList = @($module) + $pyArgs
Write-Host "Running: $py $($argList -join ' ')"
# Ensure any leftover temporary 'cmd' variable is removed to avoid "assigned but not used" warnings
Remove-Variable -Name cmd -ErrorAction SilentlyContinue
# Use Start-Process to avoid PowerShell argument quoting issues
Start-Process -NoNewWindow -Wait -FilePath $py -ArgumentList $argList
