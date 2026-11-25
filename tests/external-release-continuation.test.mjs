import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { createHash, createHmac, randomBytes } from "node:crypto";
import { cp, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";

const TOOL = path.resolve(import.meta.dirname, "..");
const APP = process.env.AGENT_HUB_RELEASE_TEST_APP && path.resolve(process.env.AGENT_HUB_RELEASE_TEST_APP);
const PYTHON = "python";
const SCRIPT = path.join(TOOL, "python", "continue_release.py");
const exec = promisify(execFile);
const put = (file, contents) => writeFile(file, JSON.stringify(contents), "utf8");
const read = (file) => readFile(file, "utf8").then(JSON.parse);

// Replace the public installer's deployment effect in this isolated fixture.
// Its Windows worker dispatch, the Hub processes and the queue are real.
function installerFixture(outcome) {
  return `import argparse, importlib.util, json, pathlib, subprocess, sys
spec=importlib.util.spec_from_file_location("public_installer", ${JSON.stringify(path.join(APP, "distribution", "install_release.py"))})
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
independent_worker=module.independent_worker
process_identity=module.process_identity
native_api=module.native_api
clean_installer_environment=module.clean_installer_environment
WorkerDispatchError=module.WorkerDispatchError
if __name__ == "__main__":
    refusal = ${JSON.stringify(outcome)}
    legacy_conflict = refusal.startswith("legacy-provider-conflict-")
    provider_refusal = legacy_conflict or refusal == "provider-refused"
    if refusal == "scratch-refused":
        raise AssertionError("the installer CLI must not start if the caller cannot create scratch")
    if refusal == "prepare-refused" or provider_refusal:
        module.__file__ = __file__
        scratch = pathlib.Path(sys.argv[sys.argv.index("--scratch-root")+1])
        operation_id = sys.argv[sys.argv.index("--operation-id")+1]
        write_record = module._write_record
        def observe():
            with (scratch.parent/"installer-observed.json").open("x") as observed:
                json.dump(dict(operation_id=operation_id), observed)
        def write_or_refuse(file, record):
            if refusal == "prepare-refused" and file.name == "request.json":
                assert file.parent.is_dir()
                assert record["operationId"] == operation_id
                observe()
                raise PermissionError("fixture cannot save the installer request")
            if provider_refusal and file.name == "worker-dispatch-failure.json":
                if not refusal.endswith("-retained"):
                    raise PermissionError("fixture cannot save the provider refusal")
            return write_record(file, record)
        def unexpected_dispatch(*args, **kwargs):
            raise AssertionError("installer worker must not launch")
        def refuse_provider(command, **kwargs):
            observe()
            reply = dict(returnValue=2, pid=123 if legacy_conflict else None)
            write_record(pathlib.Path(json.loads(kwargs["input"])["result"]), reply)
            return subprocess.CompletedProcess(command, 0, json.dumps(reply), "")
        module._write_record = write_or_refuse
        if provider_refusal:
            module.subprocess.run = refuse_provider
        else:
            module.independent_worker = unexpected_dispatch
        module.autonomous_main()
        raise AssertionError("installer should have exited with its refusal")
    p=argparse.ArgumentParser()
    for name in ("archive","sha256","runtime-root","scratch-root","operation-id","mode"):
        p.add_argument("--"+name)
    a=p.parse_args()
    scratch=pathlib.Path(a.scratch_root)
    cache=scratch/"agenthub-autonomous-fixture"
    cache.mkdir(parents=True)
    result=cache/"result.json"
    (cache/"request.json").write_text(json.dumps(dict(operationId=a.operation_id,sha256=a.sha256)))
    (scratch/"observed.json").write_text(json.dumps(vars(a)))
    if ${outcome === "succeeded" ? "True" : "False"}:
        sys.exit(19)
    print(json.dumps(dict(operationId=a.operation_id,status="dispatched",resultFile=str(result))))
`;
}

async function stageCandidate(candidate, tools, outcome, files, staged, directory) {
  const app = path.join(candidate, "AgentHub");
  const exportManifest = await read(path.join(TOOL, "export-manifest.json"));
  await mkdir(path.join(app, "distribution"), { recursive: true });
  await Promise.all(exportManifest.files.map(async name => {
    const target = path.join(tools, name);
    await mkdir(path.dirname(target), { recursive: true });
    await cp(path.join(TOOL, name), target);
  }));
  if (outcome === "build-drift") {
    const bin = path.join(tools, "bin", "release.mjs");
    await cp(bin, path.join(tools, "bin", "release-owner.mjs"));
    exportManifest.files.push("bin/release-owner.mjs");
    await put(path.join(tools, "export-manifest.json"), exportManifest);
    await writeFile(bin, `
      import { main } from "./release-owner.mjs";
      const write = process.stdout.write.bind(process.stdout);
      process.stdout.write = data => {
        const response = JSON.parse(data);
        if (process.argv[2] === "build") response.tool.sha256 = "b".repeat(64);
        return write(JSON.stringify(response) + "\\n");
      };
      await main(process.argv.slice(2));`);
  }
  await Promise.all([
    put(path.join(candidate, "fixture-files.json"), files),
    writeFile(path.join(candidate, "LICENSE"), "test license"),
    writeFile(path.join(app, "THIRD_PARTY_NOTICES.md"), "test notices"),
    writeFile(path.join(app, "distribution", "install_release.py"), installerFixture(outcome)),
    writeFile(path.join(app, "test-suite.mjs"), `
      import { appendFile, writeFile } from "node:fs/promises";
      import path from "node:path";
      const selection = JSON.parse(process.argv[2].slice(12));
      if (selection.mode !== "include" || selection.ids[0] !== "fixture:chosen") process.exit(32);
      if (${outcome === "tool-drift"}) {
        await appendFile(path.join(process.env.AGENT_HUB_RELEASE_TOOLS_ROOT, "python/build_package.py"), "\\n# replaced");
      }
      if (${outcome === "scratch-refused"}) {
        await writeFile(${JSON.stringify(path.join(directory, "installer-scratch"))}, "occupied before dispatch");
      }
      process.exit(${outcome === "failed" ? 17 : 0});`),
    writeFile(path.join(app, "build-runtime.mjs"), `
      import { cp, mkdir, readFile, writeFile } from "node:fs/promises";
      import path from "node:path";
      const args = process.argv.slice(2);
      const get = (key) => args[args.indexOf("--" + key) + 1];
      const releaseRoot = get("runtime-root"), commit = get("source-commit");
      const releaseVersion = get("artifact-version"), [version, build] = releaseVersion.split("+build.");
      const files = JSON.parse(await readFile(path.resolve("..", "fixture-files.json"), "utf8"));
      await Promise.all(files.map(async ({path: name}) => {
        const destination = path.join(releaseRoot, name);
        await mkdir(path.dirname(destination), { recursive: true });
        await cp(path.join(${JSON.stringify(staged)}, name), destination);
      }));
      await writeFile(path.join(releaseRoot, "runtime-manifest.json"), JSON.stringify({
        releaseId: get("release-id"), releaseVersion, publicationRevision: commit, version, publicVersion: version,
        versionScheme: "semver-build-v1", buildNumber: Number(build), files
      }));
      console.log(JSON.stringify({releaseRoot}));`),
  ]);
  await exec("git", ["init", "-q", candidate]);
  await exec("git", ["add", "."], { cwd: candidate });
  await exec("git", ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "candidate"], { cwd: candidate });
  return (await exec("git", ["rev-parse", "HEAD"], { cwd: candidate })).stdout.trim();
}

function active(pid) {
  try { process.kill(pid, 0); return true; } catch (error) {
    if (error.code === "ESRCH") return false;
    throw error;
  }
}

test("external release worker resumes from owner receipts and rejects tool drift", {
  skip: process.platform !== "win32" ? "Windows installer and native worker" :
    !APP ? "set AGENT_HUB_RELEASE_TEST_APP to a Hub checkout with the local continuation action and its dependencies" : false,
}, async (t) => {
  const fromApp = name => import(pathToFileURL(path.join(APP, name)).href);
  const { callDesktopControl } = await fromApp("desktop-control.mjs");
  const { CHAT_ID, E2eChildProcessOwner, FakeTelegramApi, USER_ID, waitFor } = await fromApp("e2e/telegram-harness.mjs");
  const { WorkspaceCatalogStore } = await fromApp("src/infrastructure/persistence/workspace-catalog-store.mjs");
  const { MaintenanceOperationStore } = await fromApp("src/infrastructure/persistence/maintenance-operation-store.mjs");
  const { messageCommandLedgerPath } = await fromApp("src/infrastructure/persistence/message-command-ledger-store.mjs");
  const { prepareWindowsLockDirectoryPublisher } = await fromApp("src/infrastructure/process/windows-lock-directory-publication.mjs");
  const { isRuntimeSourceFile } = await fromApp("src/infrastructure/filesystem/runtime-manifest-verifier.mjs");
  const publisher = await prepareWindowsLockDirectoryPublisher();
  t.after(() => publisher.shutdown());
  for (const outcome of ["succeeded", "retry-succeeded", "retry-queued-succeeded", "rolled-back",
    "scratch-refused", "prepare-refused", "provider-refused",
    "legacy-provider-conflict-unsaved", "legacy-provider-conflict-retained",
    "failed", "tool-drift", "build-drift"]) {
    await t.test(outcome, async (sub) => {
      const base = await mkdtemp(path.join(os.tmpdir(), "hub-release-continuation-"));
      const runtimeRoot = path.join(base, "runtime");
      const dataDir = path.join(runtimeRoot, "data");
      const turnRelease = path.join(runtimeRoot, "fixtures", "turn-release");
      const projectRoot = path.join(base, "project");
      const providerState = path.join(runtimeRoot, "fixtures", "provider.json");
      const config = path.join(runtimeRoot, "config.json");
      const connection = path.join(base, "connection.json");
      const candidate = path.join(base, "candidate");
      const tools = path.join(base, "tools");
      const directory = path.join(base, "operation");
      const retryQueued = outcome === "retry-queued-succeeded";
      const retry = outcome === "retry-succeeded" || retryQueued;
      const succeeds = outcome === "succeeded" || retry;
      const legacyConflict = outcome.startsWith("legacy-provider-conflict-");
      const readback = path.join(base, "observed-readback.json");
      const oldRelease = "old-release";
      const candidateRelease = "candidate-release";
      const version = (await read(path.join(APP, "package.json"))).version;
      const threadId = "10000000-0000-4000-8000-000000000001";
      const rollout = path.join(base, "fixture-rollout.jsonl");
      const owner = new E2eChildProcessOwner({ label: "external-release fixture Hub" });
      const telegram = new FakeTelegramApi();
      await telegram.start();
      let workerPid;
      let hubErrors = "";
      let hub;
      sub.after(async () => {
        await owner.stop();
        await telegram.stop();
        if (workerPid && active(workerPid)) process.kill(workerPid);
        if (workerPid) await waitFor(() => !active(workerPid), "external worker joined", 10_000);
        await rm(base, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
      });
      await Promise.all([
        mkdir(dataDir, { recursive: true }),
        mkdir(path.dirname(turnRelease), { recursive: true }),
        mkdir(projectRoot),
        ...[oldRelease, candidateRelease].map(name => mkdir(path.join(runtimeRoot, "releases", name), { recursive: true })),
      ]);
      const runtimeFiles = (await readdir(APP, { withFileTypes: true }))
        .filter(entry => (entry.isFile() && /\.(mjs|cjs)$/u.test(entry.name))
          || ["src", "AGENT_HUB_GUIDE.md", "package.json"].includes(entry.name)
          || (succeeds && entry.name === "assets"));
      for (const name of [oldRelease, candidateRelease]) {
        const installed = path.join(runtimeRoot, "releases", name);
        await Promise.all(runtimeFiles.map(entry => cp(path.join(APP, entry.name), path.join(installed, entry.name), {
          recursive: entry.isDirectory(),
        })));
        await symlink(path.join(APP, "node_modules"), path.join(installed, "node_modules"), "junction");
      }
      if (legacyConflict) {
        const installed = path.join(runtimeRoot, "releases", oldRelease);
        const maintenance = path.join(installed, "runtime-maintenance.mjs");
        await cp(maintenance, path.join(installed, "runtime-maintenance-owner.mjs"));
        await writeFile(maintenance, `
          import { writeFile } from "node:fs/promises";
          import { runtimeMaintenance } from "./runtime-maintenance-owner.mjs";
          let raw = "";
          for await (const chunk of process.stdin) raw += chunk;
          const input = JSON.parse(raw);
          const result = await runtimeMaintenance(process.argv[2], input);
          if (result === null) {
            await writeFile(${JSON.stringify(readback)}, JSON.stringify(input.operationId));
          }
          console.log(JSON.stringify(result));`);
      }
      const staged = path.join(runtimeRoot, "releases", candidateRelease);
      await cp(publisher.provenance.executable, path.join(staged, "ReleaseLockPublisher.exe"));
      await writeFile(path.join(staged, "ReleaseLockPublisher.source.sha256"), publisher.provenance.sourceHash);
      await writeFile(path.join(staged, "AgentHub.RuntimeHost.exe"), "inert packaging fixture");
      let runtimePaths = [
        "src/infrastructure/process/release-lock-publisher.cs",
        "ReleaseLockPublisher.exe",
        "ReleaseLockPublisher.source.sha256",
        "AgentHub.RuntimeHost.exe",
        "agent-hub-main.mjs",
      ];
      if (succeeds) {
        const entries = await readdir(staged, { recursive: true, withFileTypes: true });
        runtimePaths = [...new Set([...runtimePaths, ...entries.filter(entry => entry.isFile()).map(entry =>
          path.relative(staged, path.join(entry.parentPath, entry.name)).split(path.sep).join("/"))
          .filter(isRuntimeSourceFile)])].sort();
      }
      const manifested = await Promise.all(runtimePaths.map(async name => {
        const bytes = await readFile(path.join(staged, name));
        return { path: name, bytes: bytes.length, sha256: createHash("sha256").update(bytes).digest("hex") };
      }));
      await put(path.join(runtimeRoot, "current.json"), { current: oldRelease });
      await put(config, {
        components: { telegram: true, miniApp: false, androidApp: false },
        telegramBotToken: "123:test-token", telegramApiBaseUrl: telegram.baseUrl,
        allowedUserIds: [USER_ID], allowedChatIds: [CHAT_ID], privateChatsOnly: true,
        provider: "codex", projectRoot, projects: [{ name: "Release continuation fixture", path: projectRoot }],
        dataDir, codexExecutable: process.execPath,
        codexExecutableArgs: [path.join(APP, "e2e", "fake-codex-app-server.mjs"), "--state", providerState,
          "--require-test-contour", "--whole-release-file", turnRelease],
        codexRolloutContext: { enabled: false },
      });
      const seedIdleRollout = () => writeFile(rollout, JSON.stringify({
        timestamp: new Date().toISOString(), type: "session_meta", payload: { id: threadId, cwd: projectRoot },
      }) + "\n", "utf8");
      if (!retryQueued) await seedIdleRollout();
      await put(providerState, {
        nextThread: 2, nextTurn: 1, nextMessage: 1, requestCounts: {},
        threads: [{ id: threadId, cwd: projectRoot, name: "Release recipient", preview: "",
          path: rollout, managedRolloutPath: rollout, managedRolloutOrdinal: 1,
          updatedAt: Math.floor(Date.now() / 1000), turns: [], model: "gpt-5.6-sol", archived: false }],
      });
      await put(path.join(dataDir, "state.json"), {
        activeProvider: "codex", activeProjectRoot: projectRoot, threadIds: { codex: threadId, claude: null },
      });
      const catalog = new WorkspaceCatalogStore({ databaseFile: path.join(dataDir, "journal.sqlite") });
      let stream;
      try {
        await catalog.load();
        const workspace = catalog.createWorkspace({ name: "Release fixture", settings: { workingDirectory: projectRoot } });
        stream = catalog.createStream({ workspaceId: workspace.id, name: "Release recipient" }).id;
        catalog.updateStreamSettings(stream, { project: projectRoot });
        catalog.bindThread({ workspaceId: workspace.id, streamId: stream, provider: "codex", threadId, projectRoot });
      } finally {
        await catalog.close();
      }
      // Use a private fixture key and the real bound Stream. The running
      // service validates the signed token exactly as for its MCP transport.
      const secret = randomBytes(32).toString("base64url");
      const actor = Buffer.from(JSON.stringify({ v: 1, provider: "codex", threadId, streamId: stream })).toString("base64url");
      const contextToken = actor + "." + createHmac("sha256", secret).update(actor).digest("base64url");
      await writeFile(path.join(runtimeRoot, "hub-mcp.secret"), secret + "\n");
      await put(connection, { url: "http://127.0.0.1:1/not-used", contextToken });
      const start = (releaseId) => {
        hub = owner.start(() => spawn(process.execPath, [
          path.join(runtimeRoot, "releases", releaseId, "agent-hub-main.mjs"),
        ], {
          cwd: APP,
          env: { ...process.env, AGENT_HUB_CONFIG: config, AGENT_HUB_RUNTIME_ROOT: runtimeRoot,
            AGENT_HUB_RELEASE_ID: releaseId, AGENT_HUB_TEST_CONTOUR: "1",
            AGENT_HUB_TEST_CONTOUR_PROFILE: "fake-codex",
            CODEX_HOME: path.join(base, "codex"),
            AGENT_HUB_LAUNCHER: "", AGENT_HUB_DISABLE_SECRET_ACL: "1" },
          stdio: ["ignore", "ignore", "pipe"],
          windowsHide: true,
        }));
        hub.stderr.setEncoding("utf8").on("data", chunk => { hubErrors = (hubErrors + chunk).slice(-4000); });
      };
      const waitReady = async () => {
        const deadline = performance.now() + 25_000;
        while (hub.exitCode === null && performance.now() < deadline) {
          const health = await callDesktopControl("runtime-health", {}, { runtimeRoot, timeoutMs: 1200 }).catch(() => null);
          if (health?.ready) return health.process;
          await delay(100);
        }
        throw new Error(`Fixture Hub did not reach Ready. Exit: ${hub.exitCode}\n${hubErrors}`);
      };
      start(oldRelease);
      const initial = await waitReady();
      const launch = async (candidate, tools, directory, outcome, retryOf = []) => {
        const commit = await stageCandidate(candidate, tools, outcome, manifested, staged, directory);
        const tool = JSON.parse((await exec(process.execPath, [path.join(tools, "bin/release.mjs"), "identity"])).stdout);
        const { stdout } = await exec(PYTHON, ["-X", "utf8", "-B", SCRIPT, "start", "--directory", directory,
          "--candidate", candidate, "--commit", commit, "--tools-root", tools, "--tools-sha256", tool.sha256,
          "--runtime-root", runtimeRoot, "--release-id", candidateRelease, "--artifact-version", version + "+build.1",
          "--stream", stream, "--connection-file", connection,
          "--selection", JSON.stringify({ mode: "include", ids: ["fixture:chosen"] }),
          ...retryOf], { timeout: 30_000, windowsHide: true });
        const receipt = JSON.parse(stdout);
        workerPid = receipt.worker.pid;
        return receipt;
      };
      const resume = directory => exec(PYTHON, ["-X", "utf8", "-B", SCRIPT, "resume-delivery", directory],
        { timeout: 20_000, windowsHide: true });
      const messageReceipt = async id => {
        const snapshot = await callDesktopControl("snapshot", { messageReceiptIds: [id] }, { runtimeRoot });
        return snapshot.messageReceipts.find(item => item.clientUserMessageId === id);
      };
      const readHistory = () => callDesktopControl("task-history", { streamId: stream, limit: 10 }, { runtimeRoot });
      const closeFixtureTurn = async () => {
        await writeFile(turnRelease, "complete", "utf8");
        await waitFor(async () => {
          const snapshot = await callDesktopControl("snapshot", {}, { runtimeRoot });
          return snapshot.activeCount === 0;
        }, "fixture turn retires before replacing Hub", 15_000);
      };
      let heldTurn;
      if ((retry && !retryQueued) || outcome === "failed") {
        const holdId = "release-retry-fixture-hold";
        const hold = await callDesktopControl("send-message", {
          streamId: stream, text: "[whole-update-wait]", clientUserMessageId: holdId,
        }, { runtimeRoot });
        assert.equal(hold.accepted, true);
        heldTurn = await waitFor(async () => {
          const receipt = await messageReceipt(holdId);
          return receipt?.providerAccepted ? receipt.turnId : null;
        }, "fixture hold reaches provider", 15_000);
      }
      let failed;
      if (retry) {
        const failedDirectory = path.join(base, "failed-attempt");
        const first = await launch(path.join(base, "failed-candidate"), path.join(base, "failed-tools"), failedDirectory, "failed");
        const firstQueue = path.join(failedDirectory, "QUEUE-RECEIPT.json");
        const firstReceipt = await waitFor(() => read(firstQueue).catch(() => null), "failed-attempt queue receipt", 25_000);
        assert.equal(firstReceipt.clientUserMessageId, `mcp:${first.operationId}:continue`);
        if (retryQueued) {
          assert.equal(firstReceipt.queued, true);
          assert.equal(firstReceipt.providerAccepted, false);
        } else {
          const steered = await waitFor(async () => {
            const receipt = await messageReceipt(firstReceipt.clientUserMessageId);
            return receipt?.providerAccepted ? receipt : null;
          }, "checks failure steered to provider", 15_000);
          assert.equal(steered.turnId, heldTurn);
          assert.equal(steered.steered, true);
          assert.equal(steered.origin.kind, "interstream");
          assert.equal(steered.origin.streamId, stream);
        }
        await waitFor(() => !active(workerPid), "failed-attempt worker joined", 10_000);
        failed = { directory: failedDirectory, ...first, receipt: firstReceipt, reportBytes: await readFile(first.report) };
        assert.equal(JSON.parse(failed.reportBytes).stage, "checks");
        await rm(firstQueue);
      }
      const dispatch = await launch(candidate, tools, directory, succeeds ? "succeeded" : outcome,
        retry ? ["--retry-of", failed.directory] : []);
      if (retry) {
        const originalId = failed.receipt.clientUserMessageId;
        const old = await read(path.join(directory, "PREDECESSOR.json"));
        assert.equal(old.operationId, failed.operationId);
        assert.equal(old.notification.clientUserMessageId, originalId);
        const previous = await messageReceipt(originalId);
        assert.equal(previous.providerAccepted, !retryQueued);
        assert.equal(previous.steered, !retryQueued);
        assert.equal(old.notification.cancelled, retryQueued);
        assert.deepEqual(await readFile(failed.report), failed.reportBytes);
        if (retryQueued) await seedIdleRollout();
        else await closeFixtureTurn();
      }
      const scratch = path.join(directory, "installer-scratch");
      const installation = succeeds || outcome === "rolled-back" || legacyConflict;
      if (installation) {
        let install;
        if (legacyConflict) {
          const originalId = await waitFor(() => read(readback).catch(() => null), "absent original readback", 15_000);
          assert.equal(originalId, dispatch.operationId);
          const [cache] = (await readdir(scratch)).filter(name => name.startsWith("agenthub-autonomous-"));
          install = await read(path.join(scratch, cache, "request.json"));
          const raw = await read(path.join(scratch, cache, "worker-create-result.json"));
          assert.deepEqual(raw, { returnValue: 2, pid: 123 });
          const diagnostic = await read(path.join(directory, "install-dispatch.stderr"));
          assert.equal(diagnostic.reason, "wmi-create-refused");
          assert.equal(diagnostic.workerPid, null);
          assert.equal(diagnostic.failureRecordSaved, outcome.endsWith("-retained"));
          assert.equal((await read(path.join(directory, "installer-observed.json"))).operation_id, dispatch.operationId);
          install = { operation_id: install.operationId, mode: install.mode, sha256: install.sha256 };
        } else {
          install = await waitFor(() => read(path.join(scratch, "observed.json")).catch(() => null),
            "installer dispatch", 15_000);
        }
        assert.equal(install.operation_id, dispatch.operationId);
        assert.equal(install.mode, "wait");
        await owner.stop();
        const activeRelease = succeeds ? candidateRelease : oldRelease;
        if (succeeds) {
          const verified = path.join(base, "verified");
          await exec(PYTHON, ["-X", "utf8", "-B", "-c", `
import importlib.util, pathlib, sys
installer, archive, digest, target = sys.argv[1:]
spec = importlib.util.spec_from_file_location("published", installer)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module.extract_verified(pathlib.Path(archive), digest, pathlib.Path(target))
`, path.join(APP, "distribution", "install_release.py"), install.archive, install.sha256, verified]);
          await cp(path.join(verified, "runtime", "runtime-manifest.json"),
            path.join(runtimeRoot, "releases", activeRelease, "runtime-manifest.json"));
          await put(path.join(runtimeRoot, "current.json"), { current: activeRelease, previous: oldRelease });
        }
        const store = new MaintenanceOperationStore({ dataDir });
        store.create({
          operationId: dispatch.operationId, outcome: succeeds ? "succeeded" : "rolled-back",
          kind: "restart", streamId: "agent-hub-installer",
          threadId: runtimeRoot,
          status: succeeds ? "succeeded" : "failed",
          phase: succeeds ? "ready" : "previous-ready",
          metadata: {
            autonomousInstaller: { releaseId: candidateRelease, archiveSha256: install.sha256,
              previous: { releaseId: oldRelease } },
            [succeeds ? "autonomousReady" : "autonomousRecovery"]: { releaseId: activeRelease },
          },
        });
        for (const file of ["result.json", "failure.json"]) {
          await assert.rejects(read(path.join(scratch, "agenthub-autonomous-fixture", file)), { code: "ENOENT" });
        }
        assert.equal(active(workerPid), true, "worker survives the stopped Hub");
        await assert.rejects(read(path.join(directory, "QUEUE-RECEIPT.json")), { code: "ENOENT" });
        start(activeRelease);
        assert.notEqual((await waitReady()).pid, initial.pid);
      }
      const report = await waitFor(() => read(dispatch.report).catch(() => null), "terminal report", 25_000);
      const expectedStatus = succeeds ? "succeeded" : installation ? "rolled-back" : "failed";
      assert.equal(report.status, expectedStatus, JSON.stringify(report));
      if (installation) {
        assert.equal(report.installer.operationId, dispatch.operationId);
        assert.equal(report.installer.entityType, "maintenance-operation");
      } else if (outcome === "scratch-refused") {
        assert.equal(report.stage, "install-input");
        assert.match(report.error, /installer-scratch/u);
        await assert.rejects(readFile(path.join(directory, "install-dispatch.stderr")), { code: "ENOENT" });
        assert.equal(await readFile(scratch, "utf8"), "occupied before dispatch");
      } else if (outcome.endsWith("-refused")) {
        assert.equal(report.installer.outcome, "worker-not-resumed");
        assert.equal(report.installer.operationId, dispatch.operationId);
        const diagnostic = await read(path.join(directory, "install-dispatch.stderr"));
        assert.deepEqual(report.installer.workerFailure, diagnostic);
        assert.equal(diagnostic.operationId, dispatch.operationId);
        const installer = await readFile(path.join(directory, "install_release.py"));
        assert.equal(diagnostic.installerSha256, createHash("sha256").update(installer).digest("hex"));
        const stages = { "prepare-refused": "prepare-worker", "provider-refused": "wmi-reply" };
        assert.equal(diagnostic.stage, stages[outcome]);
        assert.equal(diagnostic.resumeAttempted, false);
        assert.equal(diagnostic.failureRecordSaved, false);
        assert.equal((await read(path.join(directory, "installer-observed.json"))).operation_id, dispatch.operationId);
        const caches = (await readdir(scratch)).filter(name => name.startsWith("agenthub-autonomous-"));
        assert.equal(caches.length, 1);
        const files = await readdir(path.join(scratch, caches[0]));
        if (outcome === "prepare-refused") assert.deepEqual(files, []);
        else {
          assert.equal(diagnostic.reason, "wmi-create-refused");
          assert.equal(diagnostic.providerReturnValue, 2);
          assert.equal(diagnostic.workerPid, null);
          assert.deepEqual(files.sort(), ["install_release.py", "request.json", "worker-create-intent.json",
            "worker-create-result.json"]);
        }
      } else {
        const stages = { failed: "checks", "tool-drift": "tool-snapshot", "build-drift": "build" };
        assert.equal(report.stage, stages[outcome]);
        assert.match(report.message, outcome === "failed" ? /17/u : /ReleaseTools/u);
        await assert.rejects(read(path.join(directory, "package.stdout")), { code: "ENOENT" });
        await assert.rejects(read(path.join(scratch, "observed.json")), { code: "ENOENT" });
      }
      assert.match(report.message, /^Продолжай выпуск\./u);
      assert.match(report.message, /Перечитай актуальные правила релизного процесса/u);
      const queuePath = path.join(directory, "QUEUE-RECEIPT.json");
      const receipt = await waitFor(() => read(queuePath).catch(() => null), "canonical queue receipt", 25_000)
        .catch(async error => {
          throw new Error(error.message + ": " + JSON.stringify(await read(path.join(directory, "QUEUE-ERROR.json")).catch(() => null)));
        });
      assert.equal(receipt.streamId, stream);
      assert.equal(receipt.clientUserMessageId, `mcp:${dispatch.operationId}:continue`);
      const current = await waitFor(async () => {
        const actual = await messageReceipt(receipt.clientUserMessageId);
        return actual?.providerAccepted ? actual : null;
      }, "notice provider acceptance", 20_000);
      assert.equal(current.origin.kind, "interstream");
      assert.equal(current.origin.streamId, stream);
      assert.match(current.author, /Release fixture - Release recipient/u);
      if (outcome === "failed") {
        assert.equal(current.turnId, heldTurn);
        assert.equal(current.steered, true);
        await assert.rejects(callDesktopControl("release-continuation", {
          streamId: stream, requestId: "foreign-capability", text: "fixture", contextToken: "tampered.invalid",
        }, { runtimeRoot }), /invalid Agent Hub context capability/u);
        assert.equal(await messageReceipt("mcp:foreign-capability"), undefined);
        await closeFixtureTurn();
        await waitFor(async () => {
          const history = await readHistory();
          return history.items.some(item => item.completed && item.messages.some(message =>
            message.clientUserMessageId === receipt.clientUserMessageId && message.origin?.kind === "interstream"));
        }, "original active turn materialized before replacing Hub", 15_000);
      } else {
        assert.equal(current.steered, false);
      }
      await waitFor(() => !active(workerPid), "worker exit", 10_000);
      if (outcome.endsWith("-drift")) return;
      // Emulate a lost local readback after canonical admission and retry that
      // same notification after the receiving process is replaced again.
      await rm(queuePath);
      const previous = await waitReady();
      await owner.stop();
      start(succeeds ? candidateRelease : oldRelease);
      assert.notEqual((await waitReady()).pid, previous.pid);
      await resume(directory);
      const replay = await read(queuePath);
      assert.equal(replay.clientUserMessageId, receipt.clientUserMessageId);
      assert.equal(replay.runId, current.runId);
      if (retry) {
        await resume(failed.directory);
        await assert.rejects(read(path.join(failed.directory, "QUEUE-RECEIPT.json")), { code: "ENOENT" });
        assert.deepEqual(await readFile(failed.report), failed.reportBytes);
      }
      if (retry || outcome === "failed") {
        const ids = [receipt.clientUserMessageId, ...(retry && !retryQueued ? [failed.receipt.clientUserMessageId] : [])];
        const messages = await waitFor(async () => {
          const history = await readHistory();
          const all = history.items.flatMap(item => item.messages);
          if (retryQueued) assert.equal(all.some(item => item.clientUserMessageId === failed.receipt.clientUserMessageId), false);
          const retained = all.filter(item => ids.includes(item.clientUserMessageId));
          return retained.length >= ids.length ? retained : null;
        }, "service notices in reopened history", 15_000);
        assert.deepEqual(messages.map(item => item.clientUserMessageId).sort(), ids.sort());
        for (const message of messages) {
          assert.equal(message.origin.kind, "interstream");
          assert.equal(message.origin.streamId, stream);
          assert.match(message.author, /Release fixture - Release recipient/u);
          assert.match(message.text, /^Продолжай выпуск\./u);
        }
      }
      const db = new DatabaseSync(messageCommandLedgerPath(dataDir), { readOnly: true });
      try {
        const query = db.prepare(`SELECT client_user_message_id id FROM message_commands WHERE client_user_message_id LIKE 'mcp:install-%:continue'
          UNION ALL SELECT client_user_message_id id FROM message_command_terminal_archive WHERE client_user_message_id LIKE 'mcp:install-%:continue'`);
        assert.deepEqual(query.all().map(row => row.id).sort(),
          [receipt.clientUserMessageId, ...(retry ? [failed.receipt.clientUserMessageId] : [])].sort());
      } finally {
        db.close();
      }
    });
  }
});
