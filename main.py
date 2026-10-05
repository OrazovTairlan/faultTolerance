import os
import subprocess
import tempfile
from pathlib import Path


# ============================================================
# SETTINGS
# ============================================================

REPO = Path.cwd()

# None = использовать текущую ветку
TARGET_BRANCH = None

REMOTE = "origin"

# True -> после переписывания истории сделать force push
PUSH_TO_REMOTE = True


# ============================================================
# GIT HELPERS
# ============================================================

def run(cmd, env=None, input_text=None):
    print(f"\n> {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        cwd=REPO,
        env=env,
        text=True,
        input=input_text,
        capture_output=True,
    )

    if result.stdout:
        print(result.stdout.strip())

    if result.returncode != 0:
        if result.stderr:
            print(result.stderr.strip())

        raise RuntimeError(
            f"Command failed with exit code {result.returncode}"
        )

    return result.stdout.strip()


def git(*args, env=None, input_text=None):
    return run(
        ["git", *args],
        env=env,
        input_text=input_text,
    )


def commit_env(date_time):
    """
    Дата и время конкретного commit.
    """

    env = os.environ.copy()

    env["GIT_AUTHOR_DATE"] = date_time
    env["GIT_COMMITTER_DATE"] = date_time

    return env


# ============================================================
# TEMP INDEX
# ============================================================

def create_empty_index():
    """
    Создает путь для временного Git index.

    Сам файл удаляется после создания имени,
    потому что git read-tree --empty должен работать
    с несуществующим index-файлом.
    """

    temp = tempfile.NamedTemporaryFile(
        prefix="git-rewrite-",
        suffix=".index",
        delete=False,
    )

    temp_path = Path(temp.name)

    temp.close()

    if temp_path.exists():
        temp_path.unlink()

    return temp_path


# ============================================================
# FILE HELPERS
# ============================================================

def existing_paths(paths):
    """
    Возвращает только существующие файлы/директории.
    """

    result = []

    for path in paths:
        full_path = REPO / path

        if full_path.exists():
            result.append(path)
        else:
            print(f"[skip] {path} does not exist")

    return result


# ============================================================
# CREATE COMMIT
# ============================================================

def create_commit(
    message,
    date_time,
    paths,
    index_env,
    parent=None,
):
    """
    Добавляет файлы во временный index,
    создает tree и commit через git commit-tree.

    Рабочая директория и основной Git index
    при этом не изменяются.
    """

    paths = existing_paths(paths)

    if not paths:
        raise RuntimeError(
            f"Нет существующих файлов для commit: {message}"
        )

    # Добавляем файлы во временный index
    git(
        "add",
        "--",
        *paths,
        env=index_env,
    )

    # Создаем tree
    tree = git(
        "write-tree",
        env=index_env,
    )

    print(f"\nTree: {tree}")

    env = commit_env(date_time)

    # Сохраняем GIT_INDEX_FILE
    env.update({
        "GIT_INDEX_FILE": index_env["GIT_INDEX_FILE"]
    })

    if parent:
        commit_hash = git(
            "commit-tree",
            tree,
            "-p",
            parent,
            env=env,
            input_text=message + "\n",
        )
    else:
        commit_hash = git(
            "commit-tree",
            tree,
            env=env,
            input_text=message + "\n",
        )

    print(f"Commit: {commit_hash}")
    print(f"Message: {message}")
    print(f"Date: {date_time}")

    return commit_hash


# ============================================================
# MAIN
# ============================================================

def main():

    # --------------------------------------------------------
    # Check repository
    # --------------------------------------------------------

    if not (REPO / ".git").exists():
        raise RuntimeError(
            "Запусти скрипт из корня Git-репозитория."
        )

    # --------------------------------------------------------
    # Current branch
    # --------------------------------------------------------

    current_branch = git(
        "branch",
        "--show-current",
    )

    if not current_branch:
        raise RuntimeError(
            "HEAD находится не на обычной ветке."
        )

    branch = TARGET_BRANCH or current_branch

    print("\n==========================================")
    print("CURRENT BRANCH")
    print("==========================================")
    print(branch)

    # --------------------------------------------------------
    # Remote
    # --------------------------------------------------------

    remotes = git("remote")

    has_remote = REMOTE in remotes.splitlines()

    if has_remote:
        print(f"\nRemote found: {REMOTE}")
    else:
        print(
            f"\nWARNING: remote '{REMOTE}' не найден."
            "\nИстория будет переписана только локально."
        )

    # --------------------------------------------------------
    # Old commit
    # --------------------------------------------------------

    old_commit = git(
        "rev-parse",
        "HEAD",
    )

    print("\n==========================================")
    print("OLD HEAD")
    print("==========================================")
    print(old_commit)

    # --------------------------------------------------------
    # Create temporary index
    # --------------------------------------------------------

    temp_index = create_empty_index()

    index_env = os.environ.copy()
    index_env["GIT_INDEX_FILE"] = str(temp_index)

    try:

        # ----------------------------------------------------
        # Empty index
        # ----------------------------------------------------

        git(
            "read-tree",
            "--empty",
            env=index_env,
        )

        print(
            "\nTemporary empty Git index created."
        )

        # ====================================================
        # 2026-10-02
        # ====================================================

        commit_1 = create_commit(
            "Initialize project structure and configuration",
            "2026-10-02 10:15:00 +0500",
            [
                ".dockerignore",
                "Dockerfile",
                "docker-compose.yml",
                "docker-compose.baseline.yml",
                "requirements.txt",
                "files.txt",
                "demo.py",
            ],
            index_env,
        )

        commit_2 = create_commit(
            "Implement baseline university services",
            "2026-10-02 17:20:00 +0500",
            [
                "baseline",
            ],
            index_env,
            parent=commit_1,
        )

        # ====================================================
        # 2026-10-03
        # ====================================================

        commit_3 = create_commit(
            "Add shared system components and monitoring",
            "2026-10-03 11:00:00 +0500",
            [
                "common",
            ],
            index_env,
            parent=commit_2,
        )

        commit_4 = create_commit(
            "Implement fault-tolerant service components",
            "2026-10-03 18:10:00 +0500",
            [
                "ft",
            ],
            index_env,
            parent=commit_3,
        )

        # ====================================================
        # 2026-10-04
        # ====================================================

        commit_5 = create_commit(
            "Add hardware redundancy and reliability components",
            "2026-10-04 10:30:00 +0500",
            [
                "hardware",
                "reliability",
            ],
            index_env,
            parent=commit_4,
        )

        commit_6 = create_commit(
            "Add deployment and service orchestration configuration",
            "2026-10-04 18:00:00 +0500",
            [
                "deploy",
                "launcher",
            ],
            index_env,
            parent=commit_5,
        )

        # ====================================================
        # 2026-10-05
        # ====================================================

        commit_7 = create_commit(
            "Add experiments and failure injection tooling",
            "2026-10-05 10:20:00 +0500",
            [
                "experiments",
                "tools",
            ],
            index_env,
            parent=commit_6,
        )

        # ----------------------------------------------------
        # Final commit
        # ----------------------------------------------------
        #
        # Добавляем всё остальное из текущего состояния проекта.
        #
        # Это позволяет включить:
        # data/
        # results/
        # tests/
        # .idea/
        # и другие текущие файлы, которые не вошли
        # в предыдущие commits.
        #
        # Git все равно пропустит файлы из .gitignore.
        # ----------------------------------------------------

        print(
            "\nAdding remaining project files..."
        )

        git(
            "add",
            "-A",
            ".",
            env=index_env,
        )

        final_tree = git(
            "write-tree",
            env=index_env,
        )

        print(
            f"\nFinal tree: {final_tree}"
        )

        final_env = commit_env(
            "2026-10-05 15:40:00 +0500"
        )

        final_env["GIT_INDEX_FILE"] = str(
            temp_index
        )

        commit_8 = git(
            "commit-tree",
            final_tree,
            "-p",
            commit_7,
            env=final_env,
            input_text=(
                "Finalize tests, results, and project artifacts\n"
            ),
        )

        print(
            f"\nFinal commit: {commit_8}"
        )

        # ----------------------------------------------------
        # Show new history
        # ----------------------------------------------------

        print(
            "\n=========================================="
        )
        print(
            "NEW HISTORY"
        )
        print(
            "==========================================\n"
        )

        git(
            "--no-pager",
            "log",
            "--reverse",
            "--date=iso",
            "--pretty=format:%h | %ad | %s",
            commit_8,
        )

        # ----------------------------------------------------
        # Replace current branch
        # ----------------------------------------------------

        print(
            "\n\n=========================================="
        )
        print(
            "REPLACING BRANCH"
        )
        print(
            "=========================================="
        )

        # Безопасно перемещаем master на новый commit.
        # old_commit используется как expected old value.
        git(
            "update-ref",
            f"refs/heads/{branch}",
            commit_8,
            old_commit,
        )

        # ----------------------------------------------------
        # Update the REAL Git index
        # ----------------------------------------------------
        #
        # Рабочую директорию НЕ переключаем.
        # Просто синхронизируем обычный index с новым HEAD.
        # ----------------------------------------------------

        git(
            "read-tree",
            commit_8,
        )

        print(
            f"\nBranch '{branch}' now points to:"
        )
        print(commit_8)

        # ----------------------------------------------------
        # Show current status
        # ----------------------------------------------------

        print(
            "\n=========================================="
        )
        print(
            "GIT STATUS"
        )
        print(
            "==========================================\n"
        )

        git("status", "--short")

        # ----------------------------------------------------
        # Force push
        # ----------------------------------------------------

        if PUSH_TO_REMOTE and has_remote:

            print(
                "\n=========================================="
            )
            print(
                "FORCE PUSH"
            )
            print(
                "=========================================="
            )

            git(
                "push",
                REMOTE,
                branch,
                "--force",
            )

            print(
                "\nForce push completed."
            )

        else:

            print(
                "\nPush disabled."
            )

        # ----------------------------------------------------
        # Final verification
        # ----------------------------------------------------

        print(
            "\n=========================================="
        )
        print(
            "FINAL HISTORY"
        )
        print(
            "==========================================\n"
        )

        git(
            "--no-pager",
            "log",
            "--oneline",
            "--date=short",
            "--format=%h | %ad | %s",
            "-10",
        )

        print(
            "\nDone."
        )

    finally:

        # ----------------------------------------------------
        # Remove temporary index
        # ----------------------------------------------------

        if temp_index.exists():
            try:
                temp_index.unlink()
                print(
                    "\nTemporary index removed."
                )
            except Exception as error:
                print(
                    "\nWARNING: Не удалось удалить "
                    f"temporary index: {error}"
                )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()