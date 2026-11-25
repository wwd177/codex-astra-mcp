<#
.SYNOPSIS
  安全增量安装 codex-astra-mcp 到本机 Google Antigravity 全局配置目录 (~/.gemini/config/)。
.DESCRIPTION
  - 复制 codex_astra_mcp/server.py 到 ~/.gemini/config/mcp/codex-astra/server.py
  - 复制 skills/neon-astra-collab/SKILL.md 到 ~/.gemini/config/skills/neon-astra-collab/SKILL.md
  - 备份并安全增量合并 ~/.gemini/config/mcp_config.json（保留所有已有 MCP 配置）
  - 绝不修改 ~/.codex/config.toml
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
  [string]$PythonPath = ""
)

$ErrorActionPreference = "Stop"

function Resolve-PythonExe {
  param([string]$Explicit)
  if ($Explicit) {
    if (-not (Test-Path $Explicit)) { throw "指定的 Python 路径不存在: $Explicit" }
    return (Resolve-Path $Explicit).Path
  }

  # 优先探测 uv 托管的独立 Python 3.12+，其次 PATH 中的 python
  $uvPyRoot = Join-Path $env:APPDATA "uv\python"
  if (Test-Path $uvPyRoot) {
    $cand = Get-ChildItem -Path $uvPyRoot -Directory -Filter "cpython-3.1*" -ErrorAction SilentlyContinue |
      Sort-Object Name -Descending |
      ForEach-Object { Join-Path $_.FullName "python.exe" } |
      Where-Object { Test-Path $_ } |
      Select-Object -First 1
    if ($cand) { return $cand }
  }

  $pyCmd = Get-Command python -ErrorAction SilentlyContinue
  if ($pyCmd -and (Test-Path $pyCmd.Source)) {
    try {
      $ver = & $pyCmd.Source --version 2>&1
      if ($LASTEXITCODE -eq 0 -and "$ver" -match "Python 3\.(1[1-9]|[2-9][0-9])") {
        return $pyCmd.Source
      }
    } catch {}
  }

  throw "未自动检测到可用的 Python 3.11+ 解释器（server.py 使用标准库 tomllib），请通过 -PythonPath 'C:\path\to\python.exe' 显式指定。"
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$srcServer = Join-Path $repoRoot "codex_astra_mcp\server.py"
$srcSkill = Join-Path $repoRoot "skills\neon-astra-collab\SKILL.md"

if (-not (Test-Path $srcServer)) { throw "缺少源文件: $srcServer" }
if (-not (Test-Path $srcSkill)) { throw "缺少源文件: $srcSkill" }

$resolvedPython = Resolve-PythonExe -Explicit $PythonPath
$configRoot = Join-Path $HOME ".gemini\config"
$dstServerDir = Join-Path $configRoot "mcp\codex-astra"
$dstServer = Join-Path $dstServerDir "server.py"
$dstSkillDir = Join-Path $configRoot "skills\neon-astra-collab"
$dstSkill = Join-Path $dstSkillDir "SKILL.md"
$mcpConfigPath = Join-Path $configRoot "mcp_config.json"

Write-Host "Python 解释器 : $resolvedPython"
Write-Host "MCP 服务目标  : $dstServer"
Write-Host "Skill 目标    : $dstSkill"
Write-Host "MCP 配置文件  : $mcpConfigPath"

if ($PSCmdlet.ShouldProcess($dstServer, "安装只读 MCP 服务脚本")) {
  New-Item -ItemType Directory -Force -Path $dstServerDir | Out-Null
  Copy-Item -Path $srcServer -Destination $dstServer -Force
}

if ($PSCmdlet.ShouldProcess($dstSkill, "安装 neon-astra-collab 双核协作 Skill")) {
  New-Item -ItemType Directory -Force -Path $dstSkillDir | Out-Null
  Copy-Item -Path $srcSkill -Destination $dstSkill -Force
}

if ($PSCmdlet.ShouldProcess($mcpConfigPath, "增量合并 codex-astra MCP 配置（保留已有服务）")) {
  New-Item -ItemType Directory -Force -Path $configRoot | Out-Null
  $cfgObj = $null
  if (Test-Path $mcpConfigPath) {
    $backupPath = "$mcpConfigPath.bak.$(Get-Date -Format 'yyyyMMdd-HHmmss')"
    Copy-Item -Path $mcpConfigPath -Destination $backupPath -Force
    Write-Host "已备份原配置  : $backupPath"
    $raw = Get-Content -Path $mcpConfigPath -Raw -Encoding UTF8
    if ($raw.Trim()) {
      $cfgObj = $raw | ConvertFrom-Json
    }
  }
  if ($null -eq $cfgObj) {
    $cfgObj = [PSCustomObject]@{ mcpServers = [PSCustomObject]@{} }
  }
  if (-not ($cfgObj.PSObject.Properties.Name -contains "mcpServers") -or $null -eq $cfgObj.mcpServers) {
    $cfgObj | Add-Member -NotePropertyName "mcpServers" -NotePropertyValue ([PSCustomObject]@{}) -Force
  }

  $entry = [PSCustomObject]@{
    command = $resolvedPython
    args    = @($dstServer)
    env     = [PSCustomObject]@{
      PYTHONIOENCODING = "utf-8"
      PYTHONUNBUFFERED = "1"
    }
    tools   = [PSCustomObject]@{
      astra_status      = [PSCustomObject]@{ eager = $true }
      astra_ask         = [PSCustomObject]@{ eager = $true }
      astra_redteam     = [PSCustomObject]@{ eager = $true }
      astra_review_diff = [PSCustomObject]@{ eager = $true }
    }
    timeoutSeconds = 360
  }

  $cfgObj.mcpServers | Add-Member -NotePropertyName "codex-astra" -NotePropertyValue $entry -Force
  $jsonOut = $cfgObj | ConvertTo-Json -Depth 10
  [System.IO.File]::WriteAllText($mcpConfigPath, $jsonOut + "`n", [System.Text.UTF8Encoding]::new($false))
}

Write-Host ""
Write-Host "安装完成！请在 Antigravity 中刷新/重连 MCP Servers 后，使用 /neon-astra-collab 启动双核协作。" -ForegroundColor Green
