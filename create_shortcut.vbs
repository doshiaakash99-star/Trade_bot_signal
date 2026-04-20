
' Script: Create Desktop Shortcut for Trading Bot
' This VBS script creates a Windows shortcut on the Desktop

Set objShell = CreateObject("WScript.Shell")
Set objFSO = CreateObject("Scripting.FileSystemObject")

' Paths
strDesktop = objShell.SpecialFolders("Desktop")
strBotDir = "C:\Users\Aakash_Doshi\Desktop\Shoonya\ShoonyaApi-py-master\ShoonyaApi-py-master"
strStartScript = strBotDir & "\START_BOT.bat"
strShortcutPath = strDesktop & "\NIFTY Trading Bot.lnk"

' Check if START_BOT.bat exists
If Not objFSO.FileExists(strStartScript) Then
    MsgBox "Error: START_BOT.bat not found at:" & vbCrLf & strStartScript, vbExclamation, "File Not Found"
    WScript.Quit 1
End If

' Create shortcut
Set objLink = objShell.CreateShortcut(strShortcutPath)
objLink.TargetPath = strStartScript
objLink.WorkingDirectory = strBotDir
objLink.Description = "NIFTY Trading Bot - Click to start market signals"
objLink.IconLocation = "C:\Windows\System32\cmd.exe,0"
objLink.Save

' Show success message
MsgBox "Shortcut created successfully!" & vbCrLf & vbCrLf & _
    "Location: " & strShortcutPath & vbCrLf & vbCrLf & _
    "You can now double-click it to start the bot.", _
    vbInformation, "Shortcut Created"
