# Управление тремя локальными копиями ZooKeeper.
#   .\zk.ps1 start 1    запустить сервер 1 штатной командой bin\zkServer.cmd (в отдельном окне)
#   .\zk.ps1 stop 1     остановить сервер 1
#   .\zk.ps1 status     роли всех серверов (четырёхбуквенная команда srvr)
param([string]$Action, [Parameter(ValueFromRemainingArguments = $true)][string[]]$Rest)
$Ids = if ($Rest) { $Rest } else { 1, 2, 3 }
$root = $PSScriptRoot
if (-not $env:JAVA_HOME) { $env:JAVA_HOME = [Environment]::GetEnvironmentVariable('JAVA_HOME', 'User') }

function ServerProcess($n) {
    Get-CimInstance Win32_Process -Filter "Name='java.exe'" |
        Where-Object { $_.CommandLine -like "*zk$n\bin\..\lib*QuorumPeerMain*" }
}

switch ($Action) {
    'start' { foreach ($n in $Ids) {
        New-Item -ItemType Directory -Force "$root\zk$n\logs" | Out-Null
        Start-Process cmd.exe -ArgumentList '/c', "title zk$n && `"$root\zk$n\bin\zkServer.cmd`" >> `"$root\zk$n\logs\console.log`" 2>&1" -WindowStyle Minimized
        "zk${n}: запущен" } }
    'stop' { foreach ($n in $Ids) {
        ServerProcess $n | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
        "zk${n}: остановлен" } }
    'status' { foreach ($n in $Ids) {
        "--- zk$n (порт 218$n) ---"
        $ErrorActionPreference = 'Continue'
        $out = & "$env:JAVA_HOME\bin\java.exe" -cp "$root\zk$n\lib\*" org.apache.zookeeper.client.FourLetterWordMain 127.0.0.1 "218$n" srvr 2>&1
        if ($LASTEXITCODE -ne 0) { 'сервер не запущен: Connection refused' }
        else { (($out | ForEach-Object { "$_" } | Where-Object { $_ -notmatch ' INFO ' }) | Out-String).Trim() } } }
    'cli' { # .\zk.ps1 cli get /path — одна команда zkCli к ансамблю (ждёт соединения с живым сервером)
        $ErrorActionPreference = 'Continue'
        & "$root\zk1\bin\zkCli.cmd" -waitforconnection -timeout 5000 -server '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183' @Rest 2>&1 |
            ForEach-Object { "$_" } | Where-Object { $_ -notmatch ' INFO |^\s*$|WATCHER::|WatchedEvent|^\tat |ConnectException|Closing socket connection' } }
}
