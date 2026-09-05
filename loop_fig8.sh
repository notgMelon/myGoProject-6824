#!/usr/bin/env bash
#
# loop_fig8.sh -- 循环运行单个 Raft Figure-8 测试，直到测试失败为止。
#
# 特性：
#   * 每次运行前临时把 raft1/util.go 里的 `const Debug = false` 改成
#     `const Debug = true`，开启 DPrintf 调试日志；脚本退出（含 Ctrl-C）时自动还原。
#   * 每个循环轮可跑一个或多个 Figure-8 测试（默认 TestFigure8，一次跑两个）；
#     可用参数传 Go 测试正则（-run 匹配），如 TestFigure8Unreliable3C。
#   * 日志写到 /tmp/loop_fig8.log，每轮覆写上一轮的日志；
#     一旦测试失败立即终止循环，失败现场的日志保留在该文件中，方便查看。
#
# 用法：
#   ./loop_fig8.sh [TestRegex]
#   ./loop_fig8.sh TestFigure8             # 默认：一次跑 TestFigure8* 全部
#   MAX_ROUNDS=100 ./loop_fig8.sh TestFigure8Unreliable3C   # 只跑 Unreliable，最多 100 轮
#
# 退出码：
#   0  = 遇到测试失败（已终止循环） / 达到 MAX_ROUNDS 仍无失败
#   1  = 环境或参数错误

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$SCRIPT_DIR/src/raft1"
UTIL="$PKG_DIR/util.go"
BAK="$UTIL.bak"

TEST_REGEX="${1:-TestFigure8}"              # go test -run 正则，默认匹配所有 Figure8 测试
LOG="/tmp/loop_fig8.log"
FAIL_LOG="/tmp/loop_fig8.FAILED.log"        # 失败时原样复制一份，防止误被覆盖
MAX_ROUNDS="${MAX_ROUNDS:-0}"               # 0 表示不限轮数，直到失败
ROUND=0

command -v go >/dev/null 2>&1 || { echo "error: 'go' not found in PATH" >&2; exit 1; }
cd "$PKG_DIR" || { echo "error: cannot cd to $PKG_DIR" >&2; exit 1; }

# ---- 退出时把 util.go 还原 ----
restore() {
    if [[ -f "$BAK" ]]; then
        mv -f "$BAK" "$UTIL"
        echo "[info] 已还原 $UTIL（Debug 恢复为 false）"
    fi
}
trap restore EXIT INT TERM

# ---- 开启 raft 的 DPrintf 调试日志 ----
if grep -q '^const Debug = false' "$UTIL"; then
    cp "$UTIL" "$BAK"
    sed -i 's/^const Debug = false/const Debug = true/' "$UTIL"
    echo "[info] 已临时开启 raft1/util.go 的 DPrintf 调试日志（结束运行自动还原）"
elif grep -q '^const Debug = true' "$UTIL"; then
    echo "[warn] $UTIL 中 Debug 已是 true，保持现状"
else
    echo "error: 无法在 $UTIL 中找到 'const Debug = false'" >&2
    exit 1
fi

# ---- 主循环：每次只跑给定测试，覆写上一轮日志，失败即停 ----
while : ; do
    ROUND=$((ROUND + 1))
    if [[ "$MAX_ROUNDS" -ne 0 && "$ROUND" -gt "$MAX_ROUNDS" ]]; then
        echo "[round $ROUND] 达到 MAX_ROUNDS=$MAX_ROUNDS 仍无失败，正常退出"
        exit 0
    fi

    echo "========== round #$ROUND ($(date '+%F %T'))  test=$TEST_REGEX =========="

    # 覆写上一轮日志：先写头部，再追加本次 go test 的全部输出（stdout+stderr）
    {
        echo "========================================"
        echo "round #$ROUND  $(date '+%F %T')  test=$TEST_REGEX"
        echo "cmd: go test -v -count=1 -run '$TEST_REGEX'"
        echo "----------------------------------------"
    } > "$LOG"

    if go test -v -count=1 -run "${TEST_REGEX}" >> "$LOG" 2>&1; then
        echo "[round $ROUND] PASS（上一轮日志已覆写：$LOG）"
    else
        rc=$?
        echo "[round $ROUND] FAILED（go test exit=$rc）—— 保留失败日志：$LOG"
        cp -f "$LOG" "$FAIL_LOG"
        echo "  失败日志备份：$FAIL_LOG"
        # 打印关键失败信息，方便快速定位
        grep -nE "FAIL|apply error|apply out of order|Fatal|panic|Log size too large" "$LOG" | tail -n 20
        exit 0
    fi
done
