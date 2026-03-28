; Acczite-Tally Connector: Windows Installer Configuration
; Created for Inno Setup 6.x

#define MyAppName "Acczite-Tally Connector"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "Acczite"
#define MyAppAuthor "OM WAGH"
#define MyAppURL "https://acczite.bizunite.in/dashboard/"
#define MyAppExeName "Acczite20.exe"
#define SourcePath "bin\Release\net8.0-windows\win-x64\publish"

[Setup]
AppId={{1DB42BDC-93B1-4A4E-AE82-C877C0D882A4}}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={autopf}\{#MyAppName}
DisableProgramGroupPage=yes
; LicenseFile=README.md ; Set to a .txt or .rtf if needed later
OutputBaseFilename=AccziteSetup
Compression=lzma
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=lowest
AppCopyright=Copyright (C) 2026 Acczite - OM WAGH
VersionInfoCompany=Acczite
VersionInfoCopyright=Copyright (C) 2026 Acczite - OM WAGH
VersionInfoDescription={#MyAppName}
VersionInfoProductName={#MyAppName}
VersionInfoProductVersion={#MyAppVersion}
OutputDir=bin\Release\Installer

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; Single-file payload includes everything (JSONs/DLLs)
Source: "{#SourcePath}\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{autoprograms}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall
