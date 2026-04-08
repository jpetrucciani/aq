import { execSync } from "node:child_process";
import footnote from "markdown-it-footnote";
import { defineConfig } from "vitepress";

function repositoryUrlFromGitRemote(): string | null {
  try {
    const remote = execSync("git config --get remote.origin.url", {
      stdio: ["ignore", "pipe", "ignore"],
    })
      .toString()
      .trim();
    if (!remote) {
      return null;
    }

    if (remote.startsWith("https://github.com/")) {
      return remote.replace(/\.git$/, "");
    }

    const sshMatch = remote.match(/^git@github\.com:(.+?)(?:\.git)?$/);
    if (sshMatch) {
      return `https://github.com/${sshMatch[1]}`;
    }

    const sshUrlMatch = remote.match(/^ssh:\/\/git@github\.com\/(.+?)(?:\.git)?$/);
    if (sshUrlMatch) {
      return `https://github.com/${sshUrlMatch[1]}`;
    }

    return null;
  } catch {
    return null;
  }
}

function normalizeBasePath(basePath: string | undefined): string {
  const trimmed = basePath?.trim();
  if (!trimmed || trimmed === "/") {
    return "/";
  }

  const withLeadingSlash = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  return withLeadingSlash.endsWith("/")
    ? withLeadingSlash
    : `${withLeadingSlash}/`;
}

const siteBasePath = normalizeBasePath(process.env.DOCS_BASE_PATH);
const iconHref = `${siteBasePath}aq-mark.svg`;
const repositoryUrl = process.env.GITHUB_REPOSITORY
  ? `https://github.com/${process.env.GITHUB_REPOSITORY}`
  : repositoryUrlFromGitRemote();

export default defineConfig({
  title: "aq",
  description: "Universal data query tool",
  base: siteBasePath,
  head: [
    ["link", { rel: "icon", type: "image/svg+xml", href: iconHref }],
    ["link", { rel: "shortcut icon", href: iconHref }],
    ["meta", { name: "theme-color", content: "#12313a" }],
  ],
  cleanUrls: true,
  lastUpdated: true,
  ignoreDeadLinks: false,
  markdown: {
    config(md) {
      md.use(footnote);
    },
  },
  themeConfig: {
    logo: {
      src: "/aq-mark.svg",
      alt: "aq",
    },
    nav: [
      { text: "Guide", link: "/" },
      { text: "Performance", link: "/performance" },
      { text: "jq Compatibility", link: "/jq-compatibility" },
      { text: "Starlark", link: "/starlark" },
    ],
    sidebar: [
      {
        text: "Overview",
        items: [
          { text: "Introduction", link: "/" },
          { text: "Performance", link: "/performance" },
          { text: "jq Compatibility", link: "/jq-compatibility" },
          { text: "Starlark", link: "/starlark" },
        ],
      },
    ],
    search: {
      provider: "local",
    },
    socialLinks: repositoryUrl ? [{ icon: "github", link: repositoryUrl }] : [],
    footer: {
      message: "Structured querying, rewriting, and Starlark scripting across common data formats.",
      copyright: "Released under the MIT License.",
    },
  },
});
