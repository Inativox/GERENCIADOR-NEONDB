; Preserve the previous local archive before the old uninstaller removes it.
; No private files are embedded in the new installer.
!define /ifndef PRIVATE_CONFIG_FOLDER "MB Finance\Gerenciador de Bases"
!macro preserveLocalArchive ROOT_KEY CONTEXT
  SetShellVarContext ${CONTEXT}
  ReadRegStr $0 ${ROOT_KEY} "${INSTALL_REGISTRY_KEY}" InstallLocation
  ${If} $0 != ""
  ${AndIf} ${FileExists} "$0\resources\app.asar"
    StrCpy $1 "$APPDATA\${PRIVATE_CONFIG_FOLDER}"
    ${IfNot} ${FileExists} "$1\legacy-app.asar"
      ClearErrors
      CreateDirectory "$1"
      CopyFiles /SILENT "$0\resources\app.asar" "$1\legacy-app.asar.tmp"
      ${If} ${Errors}
        MessageBox MB_ICONSTOP "Nao foi possivel preservar o acesso local. A atualizacao foi cancelada; a versao anterior permanece instalada."
        Abort
      ${EndIf}
      Rename "$1\legacy-app.asar.tmp" "$1\legacy-app.asar"
      ${If} ${Errors}
        MessageBox MB_ICONSTOP "Nao foi possivel salvar a configuracao anterior. A atualizacao foi cancelada."
        Abort
      ${EndIf}
    ${EndIf}
  ${EndIf}
!macroend

!macro customInit
  Push $0
  Push $1
  !insertmacro preserveLocalArchive HKCU current
  ${If} ${UAC_IsAdmin}
    !insertmacro preserveLocalArchive HKLM all
  ${EndIf}
  ${If} $installMode == "all"
    SetShellVarContext all
  ${Else}
    SetShellVarContext current
  ${EndIf}
  Pop $1
  Pop $0
!macroend
