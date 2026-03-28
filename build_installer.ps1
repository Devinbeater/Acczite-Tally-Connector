# Acczite-Tally Connector: Windows Installer Build Script
# This script automates the generation of a standalone Setup.exe using Inno Setup.

$AppProject = "Acczite20.csproj"
$PublishDir = "bin\Release\net8.0-windows\win-x64\publish"
$InnoScript = "AccziteSetup.iss"

Write-Host "[BUILD] Starting Pipeline for Acczite Installer..."

# 1. Clean previous builds
if (Test-Path $PublishDir) {
    Write-Host "[CLEAN] Removing old artifacts..."
    Remove-Item -Path $PublishDir -Recurse -Force
}

# 2. Publish as Single-File Self-Contained EXE
Write-Host "[PUBLISH] Building Self-Contained Single-File Application..."
dotnet publish $AppProject `
    -c Release `
    -r win-x64 `
    --self-contained true `
    -p:PublishSingleFile=true `
    -p:PublishReadyToRun=false `
    -p:IncludeNativeLibrariesForSelfExtract=true `
    -p:DebugType=None `
    -p:DebugSymbols=false `
    -o $PublishDir

if ($LASTEXITCODE -ne 0) {
    Write-Error "[ERROR] Dotnet publish failed."
    exit $LASTEXITCODE
}

# 3. Trigger Inno Setup Compiler
$ISCC = ""
$CommonPaths = @(
    "C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
    "C:\Program Files\Inno Setup 6\ISCC.exe",
    "$env:LOCALAPPDATA\Programs\Inno Setup 6\ISCC.exe"
)

foreach ($path in $CommonPaths) {
    if (Test-Path $path) { $ISCC = $path; break }
}

if ($ISCC -eq "" -and (Get-Command "iscc.exe" -ErrorAction SilentlyContinue)) {
    $ISCC = "iscc.exe"
}

if ($ISCC -ne "") {
    Write-Host "[ISCC] Compiling Setup Wizard using $ISCC..."
    & $ISCC $InnoScript
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[SUCCESS] Installer generated in bin\Release\Installer folder!"
    } else {
        Write-Host "[ERROR] Inno Setup compilation failed."
    }
} else {
    Write-Host "[WARNING] Inno Setup (ISCC.exe) not found."
    Write-Host "The single-file app is ready at: $PublishDir\Acczite20.exe"
    Write-Host "To build the installer, please install Inno Setup 6 and run: iscc $InnoScript"
}
