import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  COLD_HISTORY_AFTER_ARCHIVED_DAYS,
  COLD_HISTORY_AFTER_IDLE_DAYS,
  defaultAppSettings,
  type AppSettings,
  type ThreadListEntry,
} from "@bb/domain";
import {
  ChoiceDropdownSetting,
  type ChoiceDropdownOption,
} from "./ChoiceDropdownSetting";
import { Button } from "@bb/shared-ui/button";
import { ProjectSelector } from "@/components/pickers/ProjectSelector";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@bb/shared-ui/dropdown-menu";
import { EmptyStatePanel } from "@bb/shared-ui/empty-state";
import { Icon, type IconName } from "@bb/shared-ui/icon";
import { Input } from "@bb/shared-ui/input";
import { Pill } from "@bb/shared-ui/pill";
import { ThreadUnarchiveButton } from "@/components/thread/ThreadUnarchiveButton";
import { useUnarchiveThread } from "@/hooks/mutations/thread-state-mutations";
import {
  hasThreadSearchableQuery,
  useArchivedThreads,
  useThreadSearch,
  type UseArchivedThreadsFilters,
} from "@/hooks/queries/thread-queries";
import { useSidebarNavigation } from "@/hooks/queries/sidebar-navigation-query";
import type { ArchivedThreadsKindFilter } from "@/hooks/queries/query-keys";
import { getThreadRoutePath } from "@/lib/route-paths";
import { formatRelativeTime } from "@/lib/relative-time";
import { getThreadDisplayTitle } from "@/lib/thread-title";
import {
  ThreadTitle,
  useResolveThreadTitle,
} from "@/components/thread/ThreadTitleMentions";
import { CHILD_THREAD_NOUN } from "@/lib/child-thread-copy";

const ALL_PROJECTS = "all";
const ARCHIVED_THREAD_SEARCH_LIMIT = 50;

const KIND_OPTIONS: ReadonlyArray<{
  label: string;
  value: ArchivedThreadsKindFilter;
}> = [
  { label: "All threads", value: "all" },
  { label: "Threads", value: "root" },
  { label: "Child threads", value: "child" },
];

interface ArchiveFilterMenuProps<T extends string> {
  icon: IconName;
  label: string;
  options: ReadonlyArray<{ label: string; value: T }>;
  onChange: (value: T) => void;
  value: T;
}

function ArchiveFilterMenu<T extends string>({
  icon,
  label,
  onChange,
  options,
  value,
}: ArchiveFilterMenuProps<T>) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          type="button"
          variant="outline"
          className="h-8 min-w-36 justify-between gap-2 px-3 text-xs font-normal"
        >
          <span className="flex min-w-0 items-center gap-2">
            <Icon name={icon} className="size-3.5 shrink-0" />
            <span className="truncate">{label}</span>
          </span>
          <Icon name="ChevronDown" className="size-3.5 shrink-0" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-44">
        {options.map((option) => (
          <DropdownMenuItem
            key={option.value}
            onSelect={() => onChange(option.value)}
          >
            <span className="min-w-0 flex-1 truncate">{option.label}</span>
            {option.value === value ? (
              <Icon name="Check" className="size-3.5" />
            ) : null}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function filterArchivedThreadsBySearch(
  threads: ThreadListEntry[],
  search: string,
  resolveTitle: (title: string) => string,
): ThreadListEntry[] {
  const normalizedSearch = search.trim().toLocaleLowerCase();
  if (normalizedSearch.length === 0) return threads;
  return threads.filter((thread) => {
    const title = getThreadDisplayTitle(thread);
    return [title, resolveTitle(title)].some((text) =>
      text.toLocaleLowerCase().includes(normalizedSearch),
    );
  });
}

function dayCount(days: number): string {
  return days === 1 ? "1 day" : `${days} days`;
}

const NEVER_HISTORY_OPTION: ChoiceDropdownOption = {
  key: "never",
  title: "Never",
  description: "Keep this history in the database.",
};

function historyArchiveOptions(
  values: readonly number[],
  defaultValue: number | "never",
  describe: (days: number) => string,
): ChoiceDropdownOption[] {
  return [
    ...values.map((days) => ({
      key: String(days),
      title: `After ${dayCount(days)}${days === defaultValue ? " (default)" : ""}`,
      description: describe(days),
    })),
    NEVER_HISTORY_OPTION,
  ];
}

const ARCHIVED_HISTORY_OPTIONS = historyArchiveOptions(
  COLD_HISTORY_AFTER_ARCHIVED_DAYS,
  defaultAppSettings.coldHistoryAfterArchivedDays,
  (days) => `Move a thread's history once it has been archived ${dayCount(days)}.`,
);

const IDLE_HISTORY_OPTIONS = historyArchiveOptions(
  COLD_HISTORY_AFTER_IDLE_DAYS,
  defaultAppSettings.coldHistoryAfterIdleDays,
  (days) => `Move a thread's history after ${dayCount(days)} without activity.`,
);

function selectedHistoryOption(
  options: readonly ChoiceDropdownOption[],
  value: number | "never",
): ChoiceDropdownOption {
  return (
    options.find((option) => option.key === String(value)) ??
    NEVER_HISTORY_OPTION
  );
}

function parseArchivedHistoryChoice(
  key: string,
): AppSettings["coldHistoryAfterArchivedDays"] | null {
  if (key === "never") return "never";
  return (
    COLD_HISTORY_AFTER_ARCHIVED_DAYS.find((days) => String(days) === key) ?? null
  );
}

function parseIdleHistoryChoice(
  key: string,
): AppSettings["coldHistoryAfterIdleDays"] | null {
  if (key === "never") return "never";
  return COLD_HISTORY_AFTER_IDLE_DAYS.find((days) => String(days) === key) ?? null;
}

interface ArchivedThreadsSettingsSectionProps {
  disabled: boolean;
  generalSettings: AppSettings;
  onGeneralSettingsChange: (
    patch: Partial<AppSettings>,
  ) => Promise<unknown> | void;
}

function HistoryArchiveSettings({
  disabled,
  generalSettings,
  onGeneralSettingsChange,
}: ArchivedThreadsSettingsSectionProps) {
  return (
    <div className="space-y-1">
      <ChoiceDropdownSetting
        label="Archive history of archived threads"
        description="Move the event history of threads archived this long into compressed files beside the database. Nothing is lost: opening a thread restores its history first."
        triggerAriaLabel="Archive history of archived threads"
        options={ARCHIVED_HISTORY_OPTIONS}
        selected={selectedHistoryOption(
          ARCHIVED_HISTORY_OPTIONS,
          generalSettings.coldHistoryAfterArchivedDays,
        )}
        disabled={disabled}
        onSelect={(key) => {
          const value = parseArchivedHistoryChoice(key);
          if (value !== null)
            void onGeneralSettingsChange({ coldHistoryAfterArchivedDays: value });
        }}
      />
      <ChoiceDropdownSetting
        label="Archive history of inactive threads"
        description="Do the same for threads that are not archived but have had no activity this long. Pinned threads always stay in the database."
        triggerAriaLabel="Archive history of inactive threads"
        options={IDLE_HISTORY_OPTIONS}
        selected={selectedHistoryOption(
          IDLE_HISTORY_OPTIONS,
          generalSettings.coldHistoryAfterIdleDays,
        )}
        disabled={disabled}
        onSelect={(key) => {
          const value = parseIdleHistoryChoice(key);
          if (value !== null)
            void onGeneralSettingsChange({ coldHistoryAfterIdleDays: value });
        }}
      />
    </div>
  );
}

export function ArchivedThreadsSettingsSection({
  disabled,
  generalSettings,
  onGeneralSettingsChange,
}: ArchivedThreadsSettingsSectionProps) {
  const [search, setSearch] = useState("");
  const [kind, setKind] = useState<ArchivedThreadsKindFilter>("all");
  const [projectId, setProjectId] = useState(ALL_PROJECTS);
  const sidebarNavigation = useSidebarNavigation();
  const searchIsActive = hasThreadSearchableQuery(search);
  const archivedThreadsQuery = useArchivedThreads(
    {
      ...(projectId === ALL_PROJECTS ? {} : { projectId }),
      kind,
    } satisfies UseArchivedThreadsFilters,
    { enabled: !searchIsActive },
  );
  const threadSearch = useThreadSearch({
    active: searchIsActive,
    limitPerGroup: ARCHIVED_THREAD_SEARCH_LIMIT,
    query: search,
  });
  const unarchiveThread = useUnarchiveThread();
  const resolveTitle = useResolveThreadTitle();

  const projects = useMemo(() => {
    if (!sidebarNavigation.data) return [];
    return [
      sidebarNavigation.data.personalProject,
      ...sidebarNavigation.data.projects,
    ];
  }, [sidebarNavigation.data]);
  const projectNames = useMemo(
    () => new Map(projects.map((project) => [project.id, project.name])),
    [projects],
  );

  const archivedThreads = useMemo(() => {
    const threads = searchIsActive
      ? (threadSearch.data?.archived.results.map((result) => result.thread) ??
        [])
      : (archivedThreadsQuery.data?.pages ?? []).flat();
    const filteredThreads = threads.filter(
      (thread) =>
        thread.archivedAt !== null &&
        (projectId === ALL_PROJECTS || thread.projectId === projectId) &&
        (kind === "all" ||
          (thread.parentThreadId !== null) === (kind === "child")),
    );
    return searchIsActive
      ? filteredThreads
      : filterArchivedThreadsBySearch(filteredThreads, search, resolveTitle);
  }, [
    archivedThreadsQuery.data,
    kind,
    projectId,
    search,
    searchIsActive,
    threadSearch.data,
    resolveTitle,
  ]);

  const groupedThreads = useMemo(() => {
    const groups = new Map<string, ThreadListEntry[]>();
    for (const thread of archivedThreads) {
      const group = groups.get(thread.projectId);
      if (group) group.push(thread);
      else groups.set(thread.projectId, [thread]);
    }
    return [...groups.entries()];
  }, [archivedThreads]);

  const selectedKindLabel =
    KIND_OPTIONS.find((option) => option.value === kind)?.label ??
    "All threads";
  const isInitialLoading = searchIsActive
    ? threadSearch.isDebouncing ||
      (threadSearch.isLoading && threadSearch.data === undefined)
    : archivedThreadsQuery.isPending;

  return (
    <section className="space-y-5">
      <div>
        <h2 className="text-lg font-semibold text-foreground">
          Archived threads
        </h2>
        <p className="mt-1 text-xs text-subtle-foreground">
          Find and restore archived threads from across your projects.
        </p>
      </div>

      <HistoryArchiveSettings
        disabled={disabled}
        generalSettings={generalSettings}
        onGeneralSettingsChange={onGeneralSettingsChange}
      />

      <div className="flex flex-wrap items-center gap-2">
        <div className="relative min-w-56 flex-1">
          <Icon
            name="Search"
            className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-subtle-foreground"
          />
          <Input
            aria-label="Search archived threads"
            className="h-8 pl-8 text-xs"
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search archived threads…"
            value={search}
          />
        </div>
        <ArchiveFilterMenu
          icon="SlidersHorizontal"
          label={selectedKindLabel}
          onChange={setKind}
          options={KIND_OPTIONS}
          value={kind}
        />
        <ProjectSelector
          projects={projects}
          value={projectId}
          onChange={(value) => setProjectId(value ?? ALL_PROJECTS)}
          allProjectsValue={ALL_PROJECTS}
          variant="outline"
          modal={false}
          className="min-w-36 justify-between gap-2 px-3 font-normal"
        />
      </div>

      {isInitialLoading ? (
        <p className="py-8 text-center text-sm text-muted-foreground">
          Loading archived threads…
        </p>
      ) : archivedThreads.length === 0 ? (
        <EmptyStatePanel className="py-8">
          {search.trim().length > 0
            ? "No archived threads match your search."
            : "No archived threads match these filters."}
        </EmptyStatePanel>
      ) : (
        <div className="space-y-6">
          {groupedThreads.map(([groupProjectId, threads]) => (
            <section key={groupProjectId} className="space-y-2">
              <div className="flex items-center gap-2 px-0.5 text-xs font-medium text-muted-foreground">
                <Icon name="Folder" className="size-3.5" />
                <h3 className="min-w-0 flex-1 truncate">
                  {projectNames.get(groupProjectId) ?? "Unknown project"}
                </h3>
                <span>
                  {threads.length} {threads.length === 1 ? "thread" : "threads"}
                </span>
              </div>
              <div className="divide-y divide-border overflow-hidden rounded-lg border border-border bg-card">
                {threads.map((thread) => (
                  <div
                    key={thread.id}
                    className="flex min-h-14 items-center gap-3 px-4 py-2.5"
                  >
                    <Link
                      className="min-w-0 flex-1"
                      to={getThreadRoutePath({
                        projectId: thread.projectId,
                        threadId: thread.id,
                      })}
                    >
                      <span className="flex min-w-0 items-center gap-2 text-sm">
                        <ThreadTitle title={getThreadDisplayTitle(thread)} />
                        {thread.parentThreadId !== null ? (
                          <Pill variant="outline" className="shrink-0">
                            <span className="inline-flex items-center gap-1 align-middle">
                              <Icon
                                name="ChildThread"
                                className="size-3"
                                aria-hidden
                              />
                              {CHILD_THREAD_NOUN}
                            </span>
                          </Pill>
                        ) : null}
                      </span>
                      {thread.archivedAt !== null ? (
                        <span className="mt-0.5 block text-xs text-subtle-foreground">
                          Archived{" "}
                          {formatRelativeTime({
                            timestamp: thread.archivedAt,
                            now: Date.now(),
                          })}
                        </span>
                      ) : null}
                    </Link>
                    <ThreadUnarchiveButton
                      variant="secondary"
                      isPending={
                        unarchiveThread.isPending &&
                        unarchiveThread.variables?.id === thread.id
                      }
                      onUnarchive={() =>
                        unarchiveThread.mutate({ id: thread.id })
                      }
                    />
                  </div>
                ))}
              </div>
            </section>
          ))}
        </div>
      )}

      {!searchIsActive && archivedThreadsQuery.hasNextPage ? (
        <Button
          type="button"
          variant="ghost"
          className="h-9 w-full text-sm font-normal text-muted-foreground"
          disabled={archivedThreadsQuery.isFetchingNextPage}
          onClick={() => archivedThreadsQuery.fetchNextPage()}
        >
          {archivedThreadsQuery.isFetchingNextPage ? "Loading…" : "Load more"}
        </Button>
      ) : null}
    </section>
  );
}
