import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
const packageRoot = path.dirname(fileURLToPath(import.meta.url));
const [command, root, projectOption, projectRoot] = process.argv.slice(2);
if (!["install", "rollback"].includes(command) || !root
    || !(process.argv.length === 4 || process.argv.length === 6 && command === "install"
      && projectOption === "--project-root" && projectRoot)) {
  throw new Error("Usage: node install-prepared.mjs install|rollback <explicit-runtime-root> [--project-root <selected-project>]");
}
const child = spawn(process.execPath, [path.join(packageRoot, "runtime/runtime-maintenance.mjs"), "package-install"], {
  cwd: packageRoot, windowsHide: true, stdio: ["pipe", "inherit", "inherit"],
});
const joined = new Promise((resolve, reject) => {
  let launchError;
  child.once("error", error => { launchError = error; });
  child.once("close", (code, signal) => launchError ? reject(launchError) : resolve(signal ? 1 : code ?? 1));
});
child.stdin.on("error", () => {});
child.stdin.end(JSON.stringify({ command, packageRoot, runtimeRoot: path.resolve(root), ...(projectRoot ? { projectRoot } : {}) }));
process.exitCode = await joined;
