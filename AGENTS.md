# xpcode — 工作约定

- 长期工作分支固定为 `dev/swe-devin`，本仓不使用 `main`：禁止把代码合入 `main`，也禁止 checkout/reset `main`。"同步/合入 main"对本仓一律指 `dev/swe-devin`——把 `origin/main` merge 进 `dev/swe-devin` 即可。
- 提交统一署名 `wuzf`，邮箱 `wuzhangf@gmail.com`（`git -c user.name=wuzf -c user.email=wuzhangf@gmail.com`）；不用中文名、不用 devin 等工具名、不用 `wuzf@主机名` 这类衍生形式，不附 AI attribution trailer；不改全局 git config。
