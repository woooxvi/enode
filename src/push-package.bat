@echo off
chcp 65001 >nul 2>&1

setlocal enabledelayedexpansion

:: 检查INI文件是否存在
if not exist "NuGetConfig.ini" (
    echo 错误：未找到NuGetConfig.ini配置文件
    echo 请确保配置文件与本脚本在同一目录下
    pause
    exit /b 1
)

:: 从INI文件读取配置
for /f "tokens=1,2 delims==" %%a in (NuGetConfig.ini) do (
    if "%%a"=="nugetKey" set "nugetKey=%%b"
    if "%%a"=="nugetServer" set "nugetServer=%%b"
)

:: 检查必要的配置项
if "!nugetKey!"=="" (
    echo 错误：NuGetConfig.ini中未配置nugetKey
    pause
    exit /b 1
)

if "!nugetServer!"=="" (
    echo 错误：NuGetConfig.ini中未配置nugetServer
    pause
    exit /b 1
)

:: 检查是否有拖放的文件夹作为参数
set "projectDir=%~1"
if not "!projectDir!"=="" (
    :: 处理拖放的文件夹路径（转为绝对路径）
    for %%p in ("!projectDir!") do set "projectDir=%%~fp"
    if not exist "!projectDir!" (
        echo 错误：指定的项目目录不存在 - !projectDir!
        pause
        exit /b 1
    )
    call :ProcessProject "!projectDir!"
    goto :End
)

:: 没有参数，显示项目选择菜单
echo 请选择要处理的项目：
echo.

:: 获取当前目录下的子文件夹，每个子文件夹视为一个项目
set "count=0"
for /d %%d in (*) do (
    set /a count+=1
    set "project[!count!]=%%d"
    echo !count!. %%d
)

if !count! equ 0 (
    echo 错误：当前目录下未找到任何项目文件夹
    pause
    exit /b 1
)

echo.
set /p "choice=请输入项目编号 (1-!count!): "

:: 验证用户输入
if "!choice!"=="" (
    echo 错误：请输入有效的项目编号
    pause
    exit /b 1
)

for /f "delims=0123456789" %%a in ("!choice!") do (
    if not "%%a"=="" (
        echo 错误：请输入有效的数字
        pause
        exit /b 1
    )
)

if !choice! lss 1 if !choice! gtr !count! (
    echo 错误：请输入1到!count!之间的数字
    pause
    exit /b 1
)

:: 处理用户选择的项目
call :ProcessProject "!project[%choice%]!"

:End
endlocal
pause
exit /b 0

:: 处理项目的子过程
:ProcessProject
set "projDir=%~1"
echo.
echo 正在处理项目：!projDir!
echo.

:: 查找项目文件
set "projFile="
echo 正在查找项目文件...
echo 搜索路径：!projDir!

:: 先尝试直接在项目目录查找
if exist "!projDir!\*.csproj" (
    for %%f in ("!projDir!\*.csproj") do (
        if "!projFile!"=="" set "projFile=%%f"
    )
)

:: 如果没找到，尝试递归查找
if "!projFile!"=="" (
    for /r "!projDir!" %%f in (*.csproj) do (
        if "!projFile!"=="" set "projFile=%%f"
    )
)

:: 显示查找结果用于调试
if "!projFile!"=="" (
    echo 未找到任何.csproj文件
) else (
    echo 找到项目文件：!projFile!
)

if "!projFile!"=="" (
    echo 错误：在!projDir!中未找到.csproj文件
    return 1
)

echo 找到项目文件：!projFile!
echo.

:: 【核心修复：通过for命令获取标准化的输出目录，彻底避免双反斜杠】
:: 1. 先获取项目文件所在目录的绝对路径（无末尾反斜杠）
for %%F in ("!projFile!") do set "projFolder=%%~dpF"
:: 2. 拼接bin\Release路径，再通过for命令标准化（自动处理反斜杠）
for %%O in ("!projFolder!bin\Release") do set "buildOutput=%%~fpO\"
:: 最终buildOutput是标准化的绝对路径，格式如：C:\xxx\ENode.SqlServer\bin\Release\

echo 输出目录（标准化）：!buildOutput!

:: 创建输出目录（确保存在）
if not exist "!buildOutput!" mkdir "!buildOutput!"

:: 删除旧的nupkg文件
echo 正在清理旧的包文件...
del "!buildOutput!\*.nupkg" >nul 2>&1
echo.

:: 构建项目 - 使用默认输出目录
echo 正在构建项目...
dotnet build "!projFile!" --configuration Release
if %errorlevel% neq 0 (
    echo 错误：项目构建失败
    pause
    exit 2
)
echo.

:: 打包项目 - 使用与构建相同的输出目录
echo 正在打包项目...
dotnet pack "!projFile!" --configuration Release --include-symbols --output "!buildOutput!"
if %errorlevel% neq 0 (
    echo 错误：项目打包失败
    pause
    exit 3
)
echo.

:: 检查是否生成了包文件（包括符号包）
set "nupkgFile="
set "symbolsFile="
:: 【修复2：修正符号包检测逻辑，判断文件名是否包含".symbols"后缀】
for %%f in ("!buildOutput!*.nupkg") do (
    set "fileName=%%~nf"
    :: 主包：文件名不包含".symbols"
    if "!nupkgFile!"=="" if "!fileName:.symbols=!"=="!fileName!" set "nupkgFile=%%f"
    :: 符号包：文件名包含".symbols"
    if not "!fileName:.symbols=!"=="!fileName!" set "symbolsFile=%%f"
)

:: 显示找到的包文件用于调试
echo 找到的包文件：
if "!nupkgFile!"=="" (
    echo - 主包：未找到
) else (
    echo - 主包：!nupkgFile!
)

if "!symbolsFile!"=="" (
    echo - 符号包：未找到
) else (
    echo - 符号包：!symbolsFile!
)

:: 检查是否有可用的包文件
if "!nupkgFile!"=="" if "!symbolsFile!"=="" (
    echo 错误：未生成任何nupkg文件
    pause
    exit 4
)

:: 如果没有主包但有符号包，使用符号包
if "!nupkgFile!"=="" set "nupkgFile=!symbolsFile!"

:: 确认是否推送
echo.
echo 准备推送以下包：
echo !nupkgFile!
echo 到服务器：!nugetServer!
echo.
set /p "confirm=是否确认推送？(Y/N): "
if /i not "!confirm!"=="Y" (
    echo 已取消推送
    pause
    exit 5
)

:: 推送包
echo 正在推送包...
dotnet nuget push -k "!nugetKey!" "!nupkgFile!" -s "!nugetServer!"
if %errorlevel% equ 0 (
    echo 推送成功
) else (
    echo 错误：推送失败
    pause
    exit 6
)
    
