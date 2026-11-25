"""Build and install one reviewed candidate, then queue its result in Hub."""

import argparse
from contextlib import contextmanager
import errno
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
import uuid
import zipfile


DESKTOP = r'''
import path from "node:path";
import { pathToFileURL } from "node:url";
const action = process.argv[1];
let input = "";
for await (const chunk of process.stdin) input += chunk;
const { releaseRoot, runtimeRoot, commandId, params } = JSON.parse(input);
try {
  const { callDesktopControl } = await import(pathToFileURL(path.join(releaseRoot, "desktop-control.mjs")));
  console.log(JSON.stringify({ result: await callDesktopControl(action, params, { runtimeRoot, commandId }) }));
} catch (error) {
  console.log(JSON.stringify({ error: error.message, retry:
    error.desktopControlRequestMayHaveReachedServer === true ||
    ["ENOENT", "ECONNREFUSED", "EPIPE", "ECONNRESET", "EBUSY", "ETIMEDOUT"].includes(error.code) ||
    error.message === "Desktop control request timed out" }));
}
'''


class CommandFailed(RuntimeError):
    def __init__(self, returncode):
        super().__init__(f"Команда завершилась с кодом {returncode}")
        self.returncode = returncode


def read(path):
    return json.loads(path.read_bytes())


def save(path, value):
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def digest(path):
    sha256 = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def git(candidate, *args):
    return subprocess.check_output(["git", "-C", str(candidate), *args])


def assert_candidate(candidate, commit):
    if (git(candidate, "rev-parse", "HEAD").decode().strip() != commit
            or git(candidate, "status", "--porcelain=v1")):
        raise RuntimeError("Reviewed candidate checkout changed")


def load_installer(path):
    spec = importlib.util.spec_from_file_location("release_installer", path)
    installer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(installer)
    return installer


def current_release(root):
    release_id = read(root / "current.json")["current"]
    if not release_id or release_id in (".", "..") or Path(release_id).name != release_id or Path(release_id).drive:
        raise RuntimeError("Invalid installed release identity")
    return release_id


def desktop(job, action, params=None, command_id=None):
    root = Path(job["runtime_root"])
    try:
        release_root = root / "releases" / current_release(root)
    except FileNotFoundError:
        return None
    reply = subprocess.run([job["node"], "--input-type=module", "-e", DESKTOP, action],
                           input=json.dumps(dict(releaseRoot=str(release_root), runtimeRoot=str(root),
                                                 commandId=command_id, params=params or {})),
                           capture_output=True, encoding="utf-8", check=True)
    envelope = json.loads(reply.stdout)
    if envelope.get("retry"):
        return None
    if "error" in envelope:
        raise RuntimeError(envelope["error"])
    return envelope["result"]


def ready(job):
    health = desktop(job, "runtime-health")
    if (health and health.get("live") and health.get("ready") and not health.get("degraded")
            and not (health.get("installerCutover") or {}).get("admissionClosed")):
        return health["process"]
    return None


def continuation_request(job):
    return job["operation_id"] + ":continue"


def continuation_id(job):
    return "mcp:" + continuation_request(job)


def continuation_token(job):
    file = job.get("connection_file")
    if not file:
        raise RuntimeError("This attempt has no private Inter-Stream connection file")
    token = read(Path(file)).get("contextToken")
    if not isinstance(token, str) or not token.strip():
        raise RuntimeError("The private connection file has no caller capability")
    return token


@contextmanager
def continuation_lock(directory):
    import msvcrt

    # Lock one byte on a stable file. Windows releases it if its owner exits.
    with (directory / "CONTINUATION.lock").open("a+b") as lock:
        lock.seek(0)
        while True:
            try:
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
                break
            except OSError as error:
                if error.errno not in (errno.EACCES, errno.EDEADLK):
                    raise
                time.sleep(0.1)
        yield


def selected_attempt(job):
    return {"operationId": job["operation_id"], "directory": job["directory"]}


def message_receipt(job):
    command_id = continuation_id(job)
    snapshot = desktop(job, "snapshot", {"compactSnapshot": True, "messageReceiptIds": [command_id]})
    if snapshot is None:
        return None
    for receipt in snapshot["messageReceipts"]:
        if receipt["clientUserMessageId"] == command_id:
            if receipt.get("streamId") != job["stream"]:
                raise RuntimeError("Hub receipt targets a different Stream")
            return receipt
    return None


def provider_accepted(receipt):
    return (receipt.get("providerAccepted") is True or receipt.get("steered") is True
            or receipt.get("commandState") in ("started", "completed"))


def retired_receipt(receipt):
    return receipt and (provider_accepted(receipt) or receipt.get("cancelled") is True
                        or receipt.get("commandState") == "failed")


def retire_notification(previous, directory):
    command_id = continuation_id(previous)
    # Both admission and send hold the same lock. A missing local intent for a
    # managed attempt positively identifies one that has never called Hub.
    if not any((directory / name).exists() for name in ("QUEUE-INTENT.json", "QUEUE-RECEIPT.json")):
        return {"clientUserMessageId": command_id, "notSubmitted": True}
    receipt = message_receipt(previous)
    if retired_receipt(receipt):
        return receipt
    if (receipt and receipt.get("id") and not receipt.get("dispatching") and not receipt.get("unknown")
            and receipt.get("commandState") in ("accepted", "queued", "attention-required")):
        params = {"action": "cancel", "id": receipt["id"], "clientUserMessageId": command_id}
        try:
            cancelled = desktop(previous, "deferred-input", params, command_id + ":cancel")
        except RuntimeError:
            # A concurrent reservation can win this race. Only the subsequent
            # read of this original can resolve what happened.
            cancelled = None
        if (cancelled and cancelled.get("cancelled") is True and cancelled.get("id") == receipt["id"]
                and cancelled.get("clientUserMessageId") == command_id):
            return cancelled
        receipt = message_receipt(previous)
        if retired_receipt(receipt):
            return receipt
    raise RuntimeError(f"Cannot confirm the original Hub notification {command_id}; retry was not admitted")


def deliver(job, directory, report):
    receipt_path = directory / "QUEUE-RECEIPT.json"
    intent_path = directory / "QUEUE-INTENT.json"
    if "continuation_directory" not in job:
        if receipt_path.exists():
            return
        raise RuntimeError("This attempt predates Inter-Stream continuation; its original sender cannot be changed")
    chain = Path(job["continuation_directory"])
    command_id = continuation_id(job)
    delay = 2
    while True:
        with continuation_lock(chain):
            if read(chain / "CONTINUATION.json") != selected_attempt(job):
                return
            if receipt_path.exists():
                return
            if ready(job):
                submitted = intent_path.exists()
                receipt = message_receipt(job) if submitted else None
                if receipt is None:
                    params = dict(streamId=job["stream"], text=report["message"],
                                  requestId=continuation_request(job), contextToken=continuation_token(job))
                    if not submitted:
                        save(intent_path, {"clientUserMessageId": command_id})
                    result = desktop(job, "release-continuation", params, command_id)
                    if result is not None:
                        if result.get("retry") is True:
                            # The local route explicitly declined before MCP
                            # admission. Keep any earlier uncertain attempt.
                            if not submitted:
                                intent_path.unlink()
                        else:
                            receipt = result["delivery"]
                if receipt is not None:
                    if receipt.get("streamId") != job["stream"] or receipt.get("clientUserMessageId") != command_id:
                        raise RuntimeError("Hub did not confirm queue admission for the original message")
                    if receipt.get("cancelled"):
                        return
                    if receipt.get("queued") or receipt.get("steerPending") or provider_accepted(receipt):
                        save(receipt_path, receipt)
                        return
                    if receipt.get("failed") or receipt.get("rejected"):
                        raise RuntimeError(f"Hub rejected the original notification {command_id}")
        time.sleep(delay)
        delay = min(delay * 2, 15)


def verify_tool(job, root, run, stage):
    identity = json.loads(run(stage, [job["node"], root / "bin/release.mjs", "identity"]))
    if identity["sha256"] != job["tools_sha256"]:
        raise RuntimeError("ReleaseTools pin differs")


def tool_file(path):
    if os.name != "nt":
        return path
    absolute = str(path.absolute())
    if absolute.startswith("\\\\?\\"):
        return path
    if absolute.startswith("\\\\"):
        return Path("\\\\?\\UNC\\" + absolute[2:])
    return Path("\\\\?\\" + absolute)


def retain_tool(source, destination):
    manifest = read(tool_file(source / "export-manifest.json"))
    for name in dict.fromkeys([*manifest["files"], "export-manifest.json", "package.json"]):
        relative = Path(name)
        if relative.anchor or ":" in name or "\\" in name or ".." in relative.parts or not relative.parts:
            raise RuntimeError("Invalid tool export path")
        target = tool_file(destination / relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(tool_file(source / relative), target)


def package(job, directory, installer, run, candidate):
    original_tools = Path(job["tools_root"])
    verify_tool(job, original_tools, run, "tool")
    app = candidate / "AgentHub"
    run("checks", [job["node"], app / "test-suite.mjs", "--selection=" + json.dumps(job["selection"])])
    tools = directory / "release-tools"
    retain_tool(original_tools, tools)
    verify_tool(job, tools, run, "tool-snapshot")
    built = json.loads(run("build", [job["node"], tools / "bin/release.mjs", "build", "--candidate", candidate,
        "--commit", job["commit"], "--runtime-root", directory / "build", "--release-id", job["release_id"],
        "--artifact-version", job["artifact_version"]]))
    if built.get("tool", {}).get("sha256") != job["tools_sha256"]:
        raise RuntimeError("Build receipt names a different ReleaseTools export")
    built = built["result"]
    if built["candidate"]["commit"] != job["commit"]:
        raise RuntimeError("Build receipt names a different source")
    verify_tool(job, tools, run, "tool-before-package")
    recipient = directory / "recipient"
    recipient.mkdir()
    for name in ("install-prepared.mjs", "config.example.json"):
        shutil.copyfile(tools / "recipient" / name, recipient / name)
    shutil.copyfile(installer, recipient / "install_release.py")
    archive = directory / "AgentHub-win32-x64.zip"
    run("package", [sys.executable, "-X", "utf8", "-B", tools / "python/build_package.py",
        "--prepared-runtime", built["prepared"]["releaseRoot"], "--source-commit", job["commit"],
        "--license", candidate / "LICENSE", "--notices", app / "THIRD_PARTY_NOTICES.md",
        "--recipient-root", recipient, "--output", archive])
    verify_tool(job, tools, run, "tool-after-package")
    with zipfile.ZipFile(archive) as bundle:
        raw = bundle.read("runtime/runtime-manifest.json")
    manifest = json.loads(raw)
    if (manifest["releaseId"] != job["release_id"] or manifest["releaseVersion"] != job["artifact_version"]
            or manifest["publicationRevision"] != job["commit"]
            or manifest["distributionSource"]["commit"] != job["commit"]):
        raise RuntimeError("Archive identity differs from the reviewed candidate")
    return archive, dict(sha256=digest(archive), manifestSha256=hashlib.sha256(raw).hexdigest())


def read_installation(job):
    root = Path(job["runtime_root"])
    try:
        release = current_release(root)
    except FileNotFoundError:
        release = job["previous_release"]
    entry = root / "releases" / release / "runtime-maintenance.mjs"
    if not entry.is_file():
        return None
    result = subprocess.run([job["node"], str(entry), "maintenance-read"],
                            input=json.dumps(dict(runtimeRoot=str(root), operationId=job["operation_id"])),
                            capture_output=True, encoding="utf-8")
    if result.returncode:
        diagnostic = result.stderr.strip()
        if "database is locked" in diagnostic or not entry.is_file():
            return None
        raise RuntimeError(f"Installer readback failed: {diagnostic}")
    return json.loads(result.stdout)


def optional_record(path):
    try:
        return read(path)
    except (OSError, ValueError):
        # The installer writes its local evidence directly to an exclusive file.
        return None


def wmi_refusal_reported(evidence):
    return ((evidence.get("stage"), evidence.get("reason")) == ("wmi-reply", "wmi-create-refused")
            and type(evidence.get("providerExitCode")) is int and evidence["providerExitCode"] == 0
            and type(evidence.get("providerReturnValue")) is int and evidence["providerReturnValue"] > 0
            and evidence.get("workerPid") is None
            and evidence.get("workerIdentityVerified") is False)


def confirmed_wmi_refusal(evidence, cache, operation_id, installer_sha256, archive_sha256):
    if not wmi_refusal_reported(evidence):
        return False
    # Schema 1 also reports a null verified worker PID when an older
    # installer saw a positive raw PID with a nonzero provider return.
    try:
        request_bytes = (cache / "request.json").read_bytes()
        request = json.loads(request_bytes)
        intent = read(cache / "worker-create-intent.json")
        reply = read(cache / "worker-create-result.json")
    except (OSError, ValueError):
        return False
    if not all(isinstance(record, dict) for record in (request, intent, reply)):
        return False
    pid = reply.get("pid")
    return (request.get("operationId") == operation_id
            and request.get("installerSha256") == installer_sha256
            and request.get("sha256") == archive_sha256
            and type(intent.get("schema")) is int and intent["schema"] == 1
            and intent.get("mechanism") == "Win32_Process.Create"
            and intent.get("operationId") == operation_id
            and intent.get("requestSha256") == hashlib.sha256(request_bytes).hexdigest()
            and type(reply.get("returnValue")) is int and reply["returnValue"] == evidence["providerReturnValue"]
            and "pid" in reply and (pid is None or (type(pid) is int and pid == 0)))


def installer_prestart_refusal(path, operation_id, installer_sha256):
    try:
        diagnostic = read(path)
    except (OSError, ValueError):
        return None
    if not (isinstance(diagnostic, dict)
            and diagnostic.get("kind") == "installer-worker-dispatch-failed"
            and type(diagnostic.get("schema")) is int and diagnostic["schema"] == 1
            and diagnostic.get("operationId") == operation_id
            and diagnostic.get("installerSha256") == installer_sha256
            and diagnostic.get("mechanism") == "Win32_Process.Create"
            and diagnostic.get("failureRecordSaved") is False
            and diagnostic.get("workerIdentityVerified") is False
            and diagnostic.get("resumeAttempted") is False):
        return None
    stage, reason = diagnostic.get("stage"), diagnostic.get("reason")
    if stage in ("prepare-scratch", "prepare-worker") and reason == f"{stage}-failed":
        return diagnostic
    if "workerPid" in diagnostic and wmi_refusal_reported(diagnostic):
        return diagnostic
    return None


def installer_result(job, directory, installer, archive, pins, run):
    root = Path(job["runtime_root"])
    scratch = directory / "installer-scratch"
    # This caller owns scratch preparation, so a failure here proves the
    # selected installer was never invoked even when it predates typed errors.
    scratch.mkdir()
    command = [sys.executable, "-X", "utf8", "-B", installer, "--archive", archive,
               "--sha256", pins["sha256"], "--runtime-root", root, "--scratch-root", scratch,
               "--operation-id", job["operation_id"], "--mode", "wait"]
    installer_sha256 = digest(installer)
    dispatch, prestart_refusal = None, None
    try:
        dispatch = json.loads(run("install-dispatch", command))
        if dispatch["operationId"] != job["operation_id"] or dispatch["status"] != "dispatched":
            raise RuntimeError("Installer did not confirm dispatch of the original operation")
        if not Path(dispatch["resultFile"]).parent.is_relative_to(scratch):
            raise RuntimeError("Installer dispatch names a foreign result file")
    except CommandFailed as error:
        # The joined child's typed refusal can survive a failure to save local evidence.
        if error.returncode == 1:
            prestart_refusal = installer_prestart_refusal(
                directory / "install-dispatch.stderr", job["operation_id"], installer_sha256)
    except (KeyError, OSError, RuntimeError, ValueError, subprocess.SubprocessError):
        dispatch = None

    delay = 2
    while True:
        original = read_installation(job)
        if original is not None:
            if original["operationId"] != job["operation_id"]:
                raise RuntimeError("Installer readback names a different operation")
            if original["status"] in ("succeeded", "failed", "cancelled") or original.get("outcome") == "unknown":
                return original
        if prestart_refusal and prestart_refusal["stage"] != "wmi-reply" and original is None:
            return dict(operationId=job["operation_id"], status="failed", phase="installer-dispatch",
                        outcome="worker-not-resumed", workerFailure=prestart_refusal)

        # A missing console reply can still have a retained request. The
        # scratch directory belongs only to this original install command.
        caches = []
        if original is None:
            caches = ([Path(dispatch["resultFile"]).parent] if dispatch else
                      list(scratch.glob("agenthub-autonomous-*")))
        for cache in caches:
            request = optional_record(cache / "request.json")
            if (not dispatch and (not request or request.get("operationId") != job["operation_id"]
                                  or request.get("sha256") != pins["sha256"])):
                continue
            failure = optional_record(cache / "failure.json")
            refused = optional_record(cache / "worker-dispatch-failure.json")
            if failure and failure.get("effectsAdmitted") is False:
                return dict(operationId=job["operation_id"], status="failed", phase="installer-preflight",
                            outcome="preflight-failed", workerFailure=failure)
            checks = prestart_refusal or (refused.get("checks", {}) if refused else {})
            if checks.get("resumeAttempted") is not False:
                continue
            if checks.get("stage") in ("wmi-launch", "wmi-reply") and not confirmed_wmi_refusal(
                    checks, cache, job["operation_id"], installer_sha256, pins["sha256"]):
                continue
            return dict(operationId=job["operation_id"], status="failed", phase="installer-dispatch",
                        outcome="worker-not-resumed", workerFailure=prestart_refusal or refused)
        time.sleep(delay)
        delay = min(delay * 2, 15)


def verified_result(job, receipt, pins):
    if receipt["operationId"] != job["operation_id"]:
        raise RuntimeError("Installer result names a different operation")
    root = Path(job["runtime_root"])
    status, phase, outcome = (receipt.get(field) for field in ("status", "phase", "outcome"))
    metadata = receipt.get("metadata", {})
    binding = metadata.get("autonomousInstaller")
    if binding is not None and (binding.get("releaseId") != job["release_id"]
                                or binding.get("archiveSha256") != pins["sha256"]):
        raise RuntimeError("Installer binding differs from this archive")
    if (status, phase, outcome) == ("succeeded", "ready", "succeeded"):
        expected = job["release_id"]
        if (not binding or metadata.get("autonomousReady", {}).get("releaseId") != expected
                or digest(root / "releases" / expected / "runtime-manifest.json") != pins["manifestSha256"]):
            raise RuntimeError("Installed manifest or Ready evidence differs")
        state, summary = "succeeded", f"Установлена сборка {job['artifact_version']}, Hub в Ready."
    elif (status, phase, outcome) == ("failed", "previous-ready", "rolled-back"):
        expected = job["previous_release"]
        if (not binding or binding.get("previous", {}).get("releaseId") != expected
                or metadata.get("autonomousRecovery", {}).get("releaseId") != expected):
            raise RuntimeError("Rollback does not name the previous installation")
        state, summary = "rolled-back", f"Установка не удалась. Установщик вернул {expected}, Hub в Ready."
    else:
        if status == "succeeded":
            raise RuntimeError("Installer success has no Ready proof")
        state = "failed" if status in ("failed", "cancelled") else "unconfirmed"
        failure = receipt.get("workerFailure") or metadata.get("autonomousError") or metadata.get("autonomousWaitError") or {}
        summary = f"Установка: {status or state}, этап {phase or 'не подтверждён'}, исход {outcome or state}."
        if failure.get("diagnostic"):
            summary += f" {failure['diagnostic'][:400]}"
        return state, summary
    while (process := ready(job)) is None:
        time.sleep(2)
    if current_release(root) != expected or process.get("releaseId") != expected:
        raise RuntimeError("Ready Hub belongs to a different release")
    return state, summary


def save_report(job, directory, report, state, stage, summary):
    report_path = directory / "REPORT.json"
    report.update(operationId=job["operation_id"], source=job["commit"], streamId=job["stream"],
                  status=state, stage=stage,
                  message=f"Продолжай выпуск. {summary}\n[REPORT.json](<{report_path.as_posix()}>)\n"
                          "Перечитай актуальные правила релизного процесса перед продолжением.")
    save(report_path, report)


def publish_report(job, directory, report):
    try:
        deliver(job, directory, report)
    except (KeyError, OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        save(directory / "QUEUE-ERROR.json", {"operationId": job["operation_id"], "error": str(error)})
        raise


def finish(job, directory, report, state, stage, summary):
    save_report(job, directory, report, state, stage, summary)
    publish_report(job, directory, report)


def worker(directory):
    job = read(directory / "INPUT.json")
    stage = "candidate"
    installer = directory / "install_release.py"
    candidate = Path(job["candidate"])
    env = {**os.environ, "AGENT_HUB_RELEASE_TOOLS_SHA256": job["tools_sha256"]}

    def run(name, args):
        nonlocal stage
        stage = name
        local_env = {**env, "AGENT_HUB_RELEASE_TOOLS_ROOT": str(
            directory / "release-tools" if name not in ("tool", "checks") else Path(job["tools_root"]))}
        with (directory / f"{name}.stdout").open("xb") as out, (directory / f"{name}.stderr").open("xb") as err:
            process = subprocess.run([str(item) for item in args], cwd=candidate, env=local_env, stdout=out, stderr=err)
        if process.returncode:
            raise CommandFailed(process.returncode)
        return (directory / f"{name}.stdout").read_text(encoding="utf-8")

    report = {}
    try:
        assert_candidate(candidate, job["commit"])
        archive, pins = package(job, directory, installer, run, candidate)
        report.update(archive=str(archive), **pins)
        stage = "install-input"
        assert_candidate(candidate, job["commit"])
        if current_release(Path(job["runtime_root"])) != job["previous_release"]:
            raise RuntimeError("Installed Hub changed before dispatch")
        receipt = installer_result(job, directory, installer, archive, pins, run)
        report["installer"] = receipt
        stage = "verify-installation"
        state, summary = verified_result(job, receipt, pins)
    except (KeyError, OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        state = "unconfirmed" if stage in ("install-dispatch", "install-readback", "verify-installation") else "failed"
        report["error"] = str(error)
        summary = f"Этап {stage}: {error}."
    finish(job, directory, report, state, stage, summary)


def attempt_launcher(directory):
    installer = load_installer(directory / "install_release.py")
    _, _, kernel = installer.native_api()
    return installer, installer.process_identity(kernel.GetCurrentProcess())["image"]


def stage_attempt(job, target, installer_bytes):
    with TemporaryDirectory(prefix=".release-attempt-", dir=target.parent) as temporary:
        staged = Path(temporary) / "attempt"
        staged.mkdir()
        (staged / "install_release.py").write_bytes(installer_bytes)
        shutil.copyfile(__file__, staged / Path(__file__).name)
        save(staged / "INPUT.json", job)
        if job["continuation_directory"] == str(target):
            save(staged / "CONTINUATION.json", selected_attempt(job))
        launcher = attempt_launcher(staged)
        staged.rename(target)
    return launcher


def retry_attempt(job, directory, previous, previous_directory, chain, installer_bytes):
    selection_path = chain / "CONTINUATION.json"
    retry_path = chain / "CONTINUATION-RETRY.json"
    current = read(selection_path)
    original = selected_attempt(previous)
    try:
        retained = read(retry_path)
    except FileNotFoundError:
        retained = None

    runner_sha = digest(Path(__file__))
    installer_sha = hashlib.sha256(installer_bytes).hexdigest()
    if retained and retained["previous"] == original:
        pinned = retained["job"]
        if (pinned["directory"] != str(directory)
                or any(pinned[key] != value for key, value in job.items() if key != "operation_id")
                or retained["runnerSha256"] != runner_sha or retained["installerSha256"] != installer_sha):
            raise RuntimeError("Retry must resume the exact retained successor")
        if current not in (original, selected_attempt(pinned)):
            raise RuntimeError("Retry must refer to the current release attempt")
        job.update(pinned)
    else:
        if current != original or (retained and current != selected_attempt(retained["job"])):
            raise RuntimeError("Retry must refer to the current release attempt")
        if directory.exists():
            raise FileExistsError(f"Attempt directory already exists: {directory}")
        report = optional_record(previous_directory / "REPORT.json")
        if not report or report.get("operationId") != previous["operation_id"]:
            raise RuntimeError("The current release attempt has no original terminal report")
        job.update(continuation_directory=str(chain), previous_release=current_release(Path(job["runtime_root"])))
        save(retry_path, {"previous": original, "job": job, "runnerSha256": runner_sha, "installerSha256": installer_sha})

    stage_name = ".release-retry-" + hashlib.sha256(job["operation_id"].encode()).hexdigest()
    staged = directory.with_name(stage_name)
    launcher = None
    if not directory.exists() and not staged.exists() and current == original:
        launcher = stage_attempt(job, staged, installer_bytes)
    prepared = directory if directory.exists() else staged
    if (read(prepared / "INPUT.json") != job
            or digest(prepared / Path(__file__).name) != runner_sha
            or digest(prepared / "install_release.py") != installer_sha):
        raise RuntimeError("Retained successor files changed")
    installer, python = launcher or attempt_launcher(prepared)

    if current == original:
        # Re-observe the same original on every recovery. A durable cancel is
        # returned by snapshot; an earlier absence alone cannot prove no send.
        outcome = retire_notification(previous, previous_directory)
        save(prepared / "PREDECESSOR.json", {"operationId": previous["operation_id"], "notification": outcome})
        if prepared == staged:
            staged.rename(directory)
        save(selection_path, selected_attempt(job))
    return installer, python, directory / Path(__file__).name


def prepare_attempt(job, directory, candidate, previous_directory):
    installer_bytes = git(candidate, "show", f"{job['commit']}:AgentHub/distribution/install_release.py")
    if previous_directory is None:
        if directory.exists():
            raise FileExistsError(f"Attempt directory already exists: {directory}")
        job.update(continuation_directory=str(directory), previous_release=current_release(Path(job["runtime_root"])))
        installer, python = stage_attempt(job, directory, installer_bytes)
        return installer, python, directory / Path(__file__).name

    previous = read(previous_directory / "INPUT.json")
    if ("continuation_directory" not in previous or Path(previous["directory"]) != previous_directory
            or any(previous[key] != job[key] for key in ("runtime_root", "stream"))):
        raise RuntimeError("Retry must refer to a managed attempt for the same Hub and Stream")
    chain = Path(previous["continuation_directory"])
    with continuation_lock(chain):
        return retry_attempt(job, directory, previous, previous_directory, chain, installer_bytes)


def dispatch_attempt(job, directory, installer, python, script):
    chain = Path(job["continuation_directory"])
    receipt_path = directory / "DISPATCH.json"
    report_path = directory / "REPORT.json"
    try:
        with continuation_lock(chain):
            if receipt_path.exists():
                return read(receipt_path)
            if report_path.exists():
                return None
            if read(chain / "CONTINUATION.json") != selected_attempt(job):
                raise RuntimeError("Dispatch must address the current release attempt")
            # The existing native owner writes intent before calling WMI. A
            # missing reply after that cutpoint is not safe to launch again.
            if (directory / "worker-create-intent.json").exists():
                raise RuntimeError(f"Worker dispatch is unconfirmed for {job['operation_id']}")
            try:
                dispatch = installer.independent_worker(
                    [python, "-X", "utf8", "-B", str(script), "--worker", str(directory)],
                    directory, installer.clean_installer_environment(),
                )
            except installer.WorkerDispatchError as error:
                if error.checks.get("resumeAttempted") is False:
                    save_report(job, directory, {"error": str(error), "dispatchChecks": error.checks},
                                "failed", "worker-dispatch", f"Этап worker-dispatch: {error}.")
                raise
            save(receipt_path, dispatch)
            return dispatch
    except installer.WorkerDispatchError:
        if report_path.exists():
            publish_report(job, directory, read(report_path))
        raise


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--worker":
        worker(Path(sys.argv[2]).resolve())
        return
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    start = sub.add_parser("start", help="Launch an independent worker and return its dispatch receipt")
    for argument in ("candidate", "commit", "tools-sha256", "runtime-root", "directory",
                     "release-id", "artifact-version", "stream"):
        start.add_argument("--" + argument, required=True)
    start.add_argument("--connection-file", required=True, help="Private Hub connection JSON with the caller capability")
    start.add_argument("--tools-root", default=str(Path(__file__).resolve().parents[1]),
                       help="Release Tools export, defaults to this script's package")
    start.add_argument("--selection", required=True, type=json.loads, help="Existing test-suite selection JSON")
    start.add_argument("--retry-of", type=Path, help="Directory of the current attempt in this release")
    sub.add_parser("resume-delivery", help="Retry just the existing notification").add_argument("directory", type=Path)
    args = parser.parse_args()
    directory = Path(args.directory).resolve()
    if args.action == "resume-delivery":
        deliver(read(directory / "INPUT.json"), directory, read(directory / "REPORT.json"))
        return
    if not args.stream.strip():
        parser.error("Provide the target Hub Stream ID")

    job = vars(args).copy()
    previous = job.pop("retry_of")
    for key in ("candidate", "runtime_root", "tools_root", "connection_file"):
        job[key] = str(Path(job[key]).resolve(strict=True))
    candidate, root = Path(job["candidate"]), Path(job["runtime_root"])
    if directory.is_relative_to(root) or directory.is_relative_to(candidate):
        parser.error("Choose a working directory outside the candidate and installed Hub")
    assert_candidate(candidate, job["commit"])
    continuation_token(job)
    job.update(directory=str(directory), operation_id="install-" + uuid.uuid4().hex, node=shutil.which("node"))
    if not job["node"]:
        raise RuntimeError("Node.js is not available")
    installer, python, script = prepare_attempt(job, directory, candidate, previous.resolve(strict=True) if previous else None)
    dispatch = dispatch_attempt(job, directory, installer, python, script)
    print(json.dumps(dict(operationId=job["operation_id"], worker=dispatch, report=str(directory / "REPORT.json"))))


if __name__ == "__main__":
    main()
