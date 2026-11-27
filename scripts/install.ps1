<#
.SYNOPSIS
  安全增量安装 codex-astra-mcp 到本机 Google Antigravity 全局配置目录 (~/.gemini/config/)。
.DESCRIPTION
  - 复制 codex_astra_mcp/server.py 到 ~/.gemini/config/mcp/codex-astra/server.py
  - 复制 skills/neon-astra-collab/SKILL.md 到 ~/.gemini/config/skills/neon-astra-collab/SKILL.md
  - 备份并安全增量合并 ~/.gemini/config/mcp_config.json（保留所有已有 MCP 配置及用户自定义 env 覆盖项）
  - 绝不修改 ~/.codex/config.toml
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
  [string]$PythonPath = ""
)

$ErrorActionPreference = "Stop"

function Test-Python311Plus {
  param([string]$ExePath)
  if (-not $ExePath -or -not (Test-Path -LiteralPath $ExePath -PathType Leaf)) {
    return $null
  }
  try {
    $out = & $ExePath -c "import sys, tomllib; print(f'{sys.version_info[0]}.{sys.version_info[1]}.{sys.version_info[2]}')" 2>$null
    if ($LASTEXITCODE -eq 0 -and "$out" -match "^3\.(1[1-9]|[2-9][0-9])\.[0-9]+$") {
      return [version]("$out".Trim())
    }
  } catch {}
  return $null
}

function Resolve-PythonExe {
  param([string]$Explicit)
  if ($Explicit) {
    if (-not (Test-Path -LiteralPath $Explicit -PathType Leaf)) {
      throw "指定的 Python 路径不存在: $Explicit"
    }
    $resolved = (Resolve-Path -LiteralPath $Explicit).Path
    $ver = Test-Python311Plus -ExePath $resolved
    if (-not $ver) {
      throw "指定的 Python 无法正常执行或版本低于 3.11（需标准库 tomllib）: $resolved"
    }
    return $resolved
  }

  # 1. 优先探测 uv 托管的独立 Python 3.11+（按真实语义版本号降序，排除 3.10 与字典序错排）
  $uvPyRoot = Join-Path $env:APPDATA "uv\python"
  if (Test-Path -LiteralPath $uvPyRoot -PathType Container) {
    $validUv = @()
    $dirs = Get-ChildItem -LiteralPath $uvPyRoot -Directory -Filter "cpython-3.*" -ErrorAction SilentlyContinue
    foreach ($d in $dirs) {
      $exe = Join-Path $d.FullName "python.exe"
      $ver = Test-Python311Plus -ExePath $exe
      if ($ver) {
        $validUv += [PSCustomObject]@{ Path = $exe; Version = $ver }
      }
    }
    if ($validUv.Count -gt 0) {
      return ($validUv | Sort-Object Version -Descending | Select-Object -First 1).Path
    }
  }

  # 2. 遍历 PATH 中全部 python 可执行文件（跳过损坏的 WindowsApps stub）
  $pyCmds = Get-Command python -All -ErrorAction SilentlyContinue
  foreach ($cmd in $pyCmds) {
    $src = $cmd.Source
    $ver = Test-Python311Plus -ExePath $src
    if ($ver) {
      return $src
    }
  }

  throw "未自动检测到可用的 Python 3.11+ 解释器（server.py 使用标准库 tomllib），请通过 -PythonPath 'C:\path\to\python.exe' 显式指定。"
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$srcServer = Join-Path $repoRoot "codex_astra_mcp\server.py"
$srcSkill = Join-Path $repoRoot "skills\neon-astra-collab\SKILL.md"

if (-not (Test-Path -LiteralPath $srcServer -PathType Leaf)) { throw "缺少源文件: $srcServer" }
if (-not (Test-Path -LiteralPath $srcSkill -PathType Leaf)) { throw "缺少源文件: $srcSkill" }

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
  Copy-Item -LiteralPath $srcServer -Destination $dstServer -Force
}

if ($PSCmdlet.ShouldProcess($dstSkill, "安装 neon-astra-collab 双核协作 Skill")) {
  New-Item -ItemType Directory -Force -Path $dstSkillDir | Out-Null
  Copy-Item -LiteralPath $srcSkill -Destination $dstSkill -Force
}

if ($PSCmdlet.ShouldProcess($mcpConfigPath, "增量合并 codex-astra MCP 配置（保留已有服务与自定义 env）")) {
  New-Item -ItemType Directory -Force -Path $configRoot | Out-Null
  $cfgObj = $null
  if (Test-Path -LiteralPath $mcpConfigPath -PathType Leaf) {
    $backupPath = "$mcpConfigPath.bak.$(Get-Date -Format 'yyyyMMdd-HHmmss')"
    Copy-Item -LiteralPath $mcpConfigPath -Destination $backupPath -Force
    Write-Host "已备份原配置  : $backupPath"
    $raw = Get-Content -LiteralPath $mcpConfigPath -Raw -Encoding UTF8
    if ($raw -and $raw.Trim()) {
      try {
        $cfgObj = $raw | ConvertFrom-Json
      } catch {
        throw "现有 mcp_config.json JSON 格式无效，已终止覆盖以保护配置（备份位于 $backupPath）: $_"
      }
    }
  }
  if ($null -eq $cfgObj) {
    $cfgObj = [PSCustomObject]@{ mcpServers = [PSCustomObject]@{} }
  }
  if (-not ($cfgObj.PSObject.Properties.Name -contains "mcpServers") -or $null -eq $cfgObj.mcpServers) {
    $cfgObj | Add-Member -NotePropertyName "mcpServers" -NotePropertyValue ([PSCustomObject]@{}) -Force
  }

  # 增量保留已有 codex-astra 节点的自定义环境变量（如 ASTRA_DEFAULT_EFFORT、CODEX_BIN）
  $envObj = [PSCustomObject]@{
    PYTHONIOENCODING = "utf-8"
    PYTHONUNBUFFERED = "1"
  }
  if ($cfgObj.mcpServers.PSObject.Properties.Name -contains "codex-astra" -and $null -ne $cfgObj.mcpServers."codex-astra") {
    $oldEntry = $cfgObj.mcpServers."codex-astra"
    if ($oldEntry.PSObject.Properties.Name -contains "env" -and $null -ne $oldEntry.env) {
      foreach ($prop in $oldEntry.env.PSObject.Properties) {
        $envObj | Add-Member -NotePropertyName $prop.Name -NotePropertyValue $prop.Value -Force
      }
    }
  }

  $entry = [PSCustomObject]@{
    command = $resolvedPython
    args    = @($dstServer)
    env     = $envObj
    tools   = [PSCustomObject]@{
      astra_status      = [PSCustomObject]@{ eager = $true }
      astra_ask         = [PSCustomObject]@{ eager = $true }
      astra_redteam     = [PSCustomObject]@{ eager = $true }
      astra_review_diff = [PSCustomObject]@{ eager = $true }
    }
    timeoutSeconds = 600
  }

  $cfgObj.mcpServers | Add-Member -NotePropertyName "codex-astra" -NotePropertyValue $entry -Force
  $jsonOut = $cfgObj | ConvertTo-Json -Depth 10
  [System.IO.File]::WriteAllText($mcpConfigPath, $jsonOut + "`n", [System.Text.UTF8Encoding]::new($false))
}

Write-Host ""
Write-Host "安装完成！请在 Antigravity 中刷新/重连 MCP Servers 后，使用 /neon-astra-collab 启动双核协作。" -ForegroundColor Green
