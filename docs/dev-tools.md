# 🧰 Nx Monorepo Developer Cheat Sheet

> 🏗️ For the **Jobber Monorepo** — projects like `jobber-auth`, `jobber-jobs`, `jobber-executor`, and shared libs (`grpc`, `pulsar`, etc.)

---

<details>
<summary>🚀 <strong>Core Development Commands</strong></summary>

| Command              | Description                    | Example                   |
| -------------------- | ------------------------------ | ------------------------- |
| `nx serve <app>`     | Start a NestJS app in dev mode | `nx serve jobber-auth`    |
| `nx build <project>` | Build into `dist/`             | `nx build jobber-jobs`    |
| `nx test <project>`  | Run Jest tests                 | `nx test jobber-executor` |
| `nx lint <project>`  | Run ESLint                     | `nx lint jobber-auth`     |
| `nx format:write`    | Format all code using Prettier | `nx format:write`         |

</details>

---

<details>
<summary>🧠 <strong>Dependency Awareness & Graphs</strong></summary>

| Command                      | Description                       | Example                        |
| ---------------------------- | --------------------------------- | ------------------------------ |
| `nx graph`                   | Open interactive dependency graph | `nx graph`                     |
| `nx graph --focus=<project>` | Focus on a project and its deps   | `nx graph --focus=jobber-auth` |
| `nx show projects`           | List all Nx projects              | `nx show projects`             |
| `nx show project <name>`     | Show one project’s details        | `nx show project grpc`         |
| `nx graph --file deps.html`  | Save graph to an HTML file        | `nx graph --file deps.html`    |

💡 Use `nx graph --focus=<app>` to visualize only what’s relevant when debugging cross-lib dependencies.

</details>

---

<details>
<summary>🧩 <strong>Code Generation (Scaffolding)</strong></summary>

| Command                                        | Description                  | Example                                             |
| ---------------------------------------------- | ---------------------------- | --------------------------------------------------- |
| `nx g @nx/nest:module <name> --project=<app>`  | Generate a new NestJS module | `nx g @nx/nest:module auth --project=jobber-auth`   |
| `nx g @nx/nest:service <name> --project=<app>` | Generate a new service       | `nx g @nx/nest:service users --project=jobber-auth` |
| `nx g @nx/js:lib <name>`                       | Create a shared library      | `nx g @nx/js:lib pulsar`                            |

💡 Nx generators automatically respect monorepo boundaries, tags, and folder structure.

</details>

---

<details>
<summary>🧮 <strong>Smart “Affected” Commands (for CI/CD)</strong></summary>

| Command             | Description                                   | Example                                     |
| ------------------- | --------------------------------------------- | ------------------------------------------- |
| `nx affected:build` | Build only changed projects since base branch | `nx affected:build`                         |
| `nx affected:test`  | Run tests only for changed projects           | `nx affected:test`                          |
| `nx affected:lint`  | Lint changed projects                         | `nx affected:lint`                          |
| `nx print-affected` | See what Nx would rebuild                     | `nx print-affected --base=main --head=HEAD` |

💡 Great for GitHub Actions — only re-build or test what actually changed.

</details>

---

<details>
<summary>🏗️ <strong>Multi-Project & Batch Execution</strong></summary>

| Command                                    | Description                         | Example                                   |
| ------------------------------------------ | ----------------------------------- | ----------------------------------------- |
| `nx run-many -t <target>`                  | Run target across multiple projects | `nx run-many -t build --all`              |
| `nx run-many -t <target> -p <proj1,proj2>` | Run target for selected projects    | `nx run-many -t test -p jobber-auth,grpc` |
| `nx run-many -t build --parallel=3`        | Parallelize builds                  | `nx run-many -t build --parallel=3`       |

💡 Use in CI for fast parallelized builds.

</details>

---

<details>
<summary>💾 <strong>Caching & Maintenance</strong></summary>

| Command                  | Description                      | Example                  |
| ------------------------ | -------------------------------- | ------------------------ |
| `nx reset`               | Clear Nx daemon and local cache  | `nx reset`               |
| `nx format:check`        | Check for formatting issues      | `nx format:check`        |
| `nx connect-to-nx-cloud` | Enable Nx Cloud (shared caching) | `nx connect-to-nx-cloud` |

💡 Run `nx reset` when builds stop auto-refreshing or seem “stuck.”

</details>

---

<details>
<summary>🧭 <strong>Info, Inspection & Debugging</strong></summary>

| Command                  | Description                   | Example                           |
| ------------------------ | ----------------------------- | --------------------------------- |
| `nx list`                | List installed Nx plugins     | `nx list`                         |
| `nx list @nx/nest`       | Show Nest-specific generators | `nx list @nx/nest`                |
| `nx report`              | Print environment info        | `nx report`                       |
| `nx show project <name>` | Show project details          | `nx show project jobber-executor` |

💡 Use `nx report` when debugging Nx or CI builds.

</details>

---

<details>
<summary>⚙️ <strong>Migration & Advanced Maintenance</strong></summary>

| Command                       | Description                                         | Example                       |
| ----------------------------- | --------------------------------------------------- | ----------------------------- |
| `nx migrate latest`           | Update Nx + plugins to latest versions              | `nx migrate latest`           |
| `nx migrate --run-migrations` | Apply generated migrations                          | `nx migrate --run-migrations` |
| `nx infer`                    | Rebuild dependency graph if Nx missed a new project | `nx infer`                    |

💡 Nx migrations automatically update configs, executors, and schematics safely.

</details>

---

<details>
<summary>🧾 <strong>Advanced Usage Patterns</strong></summary>

| Pattern                              | Meaning                                            |
| ------------------------------------ | -------------------------------------------------- |
| `^build`                             | Build all dependency libraries before this project |
| `nx run-many -t <target> --all`      | Run a target for every project                     |
| `nx affected:*`                      | Run only for changed projects                      |
| `nx build <project> --skip-nx-cache` | Force rebuild even if cached                       |
| `nx graph --focus=<app>`             | Visualize dependency scope for one project         |

</details>

---

## 💡 Quick Reference

```bash
# Show dependency graph
nx graph

# Build and serve
nx build jobber-auth
nx serve jobber-auth

# Test and lint
nx test jobber-jobs
nx lint jobber-auth

# Generate new code
nx g @nx/nest:module auth --project=jobber-auth
nx g @nx/nest:service users --project=jobber-auth

# Build only changed projects
nx affected:build

# Reset cache
nx reset
```
