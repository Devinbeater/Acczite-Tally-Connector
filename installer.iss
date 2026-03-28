; Acczite 2.0 - Professional Installer Script
; Built with Inno Setup 6

#define AppName "Acczite"
#define AppVersion "2.0.0"
#define AppPublisher "Bizunite Tech Ventures Pvt Ltd"
#define AppURL "https://acczite.com"
#define AppExeName "Acczite20.exe"
#define AppCopyright "Copyright (C) 2025-2026 Bizunite Tech Ventures Pvt Ltd"

[Setup]
AppId=Acczite.Enterprise.SyncHub.20
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppURL}
AppSupportURL={#AppURL}
AppUpdatesURL={#AppURL}
AppCopyright={#AppCopyright}
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
SetupIconFile=Assets\app.ico
UninstallDisplayIcon={app}\{#AppExeName}
UninstallDisplayName={#AppName}
OutputDir=installer_output
OutputBaseFilename=Acczite_Setup_{#AppVersion}
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
WizardImageFile=compiler:wizmodernimage.bmp
WizardSmallImageFile=compiler:wizmodernsmallimage.bmp
VersionInfoVersion={#AppVersion}.0
VersionInfoCompany={#AppPublisher}
VersionInfoDescription={#AppName} Enterprise Data Platform
VersionInfoCopyright={#AppCopyright}
VersionInfoProductName={#AppName}
VersionInfoProductVersion={#AppVersion}
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
DisableProgramGroupPage=yes
PrivilegesRequired=admin
MinVersion=10.0
LicenseFile=
; Branding
WizardImageAlphaFormat=defined

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: checkedonce
Name: "quicklaunchicon"; Description: "Pin to &Taskbar"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; All published files
Source: "publish\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
; Config files (don't overwrite existing on upgrade)
Source: "appsettings.json"; DestDir: "{app}"; Flags: onlyifdoesntexist
Source: "Assets\app.ico"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
; Start Menu shortcut
Name: "{autoprograms}\{#AppName}"; Filename: "{app}\{#AppExeName}"; IconFilename: "{app}\app.ico"; Comment: "Launch {#AppName} Enterprise Hub"
; Desktop shortcut
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExeName}"; IconFilename: "{app}\app.ico"; Tasks: desktopicon; Comment: "Launch {#AppName} Enterprise Hub"

[Run]
Filename: "{app}\{#AppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(AppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{app}\logs"
Type: filesandordirs; Name: "{app}\cache"

[Code]
// Custom installer page behavior
procedure InitializeWizard;
begin
  WizardForm.WelcomeLabel2.Caption := 
    'Setup will install {#AppName} {#AppVersion} on your computer.' + #13#10 + #13#10 +
    '{#AppName} is an enterprise-grade Tally synchronization and business intelligence platform.' + #13#10 + #13#10 +
    'Features:' + #13#10 +
    '  • Real-time Tally data synchronization' + #13#10 +
    '  • Voucher Explorer & Daybook' + #13#10 +
    '  • Trial Balance, P&L, Balance Sheet' + #13#10 +
    '  • GST Reporting & Inventory Management' + #13#10 +
    '  • Enterprise-grade security & licensing' + #13#10 + #13#10 +
    'Click Next to continue, or Cancel to exit Setup.';
end;

function NeedRestart(): Boolean;
begin
  Result := False;
end;
