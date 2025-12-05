import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";
import * as fs from "fs";
import * as path from "path";

type StartListEntry = {
  raceId: string;
  racerId: string;
  racerName: string;
  teamId: string;
  teamName: string;
  gender: string;
  class: string;
  bib: number;
};

type CliOptions = {
  raceId: string;
  file: string;
  output?: string;
  region?: string;
  startListsTable?: string;
};

function parseArgs(argv: string[]): CliOptions {
  const opts: Partial<CliOptions> = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    const takeVal = (): [string | undefined, number] => {
      if (arg.includes("=")) return [arg.split("=").slice(1).join("="), i];
      const next = argv[i + 1];
      if (next && !next.startsWith("--")) return [next, i + 1];
      return [undefined, i];
    };
    if (arg.startsWith("--race-id")) {
      const [v, nextI] = takeVal();
      opts.raceId = v;
      i = nextI;
    } else if (arg.startsWith("--file")) {
      const [v, nextI] = takeVal();
      opts.file = v;
      i = nextI;
    } else if (arg.startsWith("--output")) {
      const [v, nextI] = takeVal();
      opts.output = v;
      i = nextI;
    } else if (arg.startsWith("--region")) {
      const [v, nextI] = takeVal();
      opts.region = v;
      i = nextI;
    } else if (arg.startsWith("--startlists-table")) {
      const [v, nextI] = takeVal();
      opts.startListsTable = v;
      i = nextI;
    }
  }

  if (!opts.raceId) throw new Error("Missing required --race-id");
  if (!opts.file) throw new Error("Missing required --file");
  return opts as CliOptions;
}

function normalizeRacerName(name: string | undefined): string {
  if (!name) return "";
  const raw = name.trim();
  const lastCommaIdx = raw.indexOf(",");
  const flipped = lastCommaIdx >= 0
    ? `${raw.slice(lastCommaIdx + 1)} ${raw.slice(0, lastCommaIdx)}`
    : raw;
  return flipped
    .replace(/[^a-z0-9\s]/gi, " ")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeTeamName(team: string | undefined): string {
  return (team || "")
    .replace(/[^a-z0-9]/gi, "")
    .toLowerCase();
}

async function fetchStartList(opts: CliOptions): Promise<StartListEntry[]> {
  const region = opts.region || process.env.AWS_REGION;
  const tableName = opts.startListsTable || process.env.STARTLISTS_TABLE || "StartLists";
  const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({ region }));
  const res = await ddb.send(new QueryCommand({
    TableName: tableName,
    KeyConditionExpression: "raceId = :r",
    ExpressionAttributeValues: { ":r": opts.raceId },
  }));
  const entries: StartListEntry[] = (res.Items ?? [])
    .filter(i => typeof i.bib === "number" && i.bib > 0)
    .map((i) => ({
      raceId: String(i.raceId),
      racerId: String(i.racerId),
      racerName: String(i.racerName),
      teamId: String(i.teamId),
      teamName: String(i.teamName),
      gender: String(i.gender),
      class: String(i.class),
      bib: Number(i.bib),
    }))
    .sort((a, b) => a.bib - b.bib);
  return entries;
}

function buildEntryMaps(entries: StartListEntry[]) {
  const byNameTeam = new Map<string, StartListEntry>();
  const byName = new Map<string, StartListEntry[]>();
  for (const e of entries) {
    const normName = normalizeRacerName(e.racerName);
    const normTeam = normalizeTeamName(e.teamName);
    const key = `${normName}|${normTeam}`;
    if (!byNameTeam.has(key)) byNameTeam.set(key, e);
    const arr = byName.get(normName) || [];
    arr.push(e);
    byName.set(normName, arr);
  }
  return { byNameTeam, byName };
}

function updateNatfis(xml: string, entries: StartListEntry[]) {
  const { byNameTeam, byName } = buildEntryMaps(entries);
  const used = new Set<string>();
  const unmatchedComps: { name: string; team: string }[] = [];
  const compRegex = /<Comp>[\s\S]*?<\/Comp>/g;
  const updatedBlocks: string[] = [];

  for (const match of xml.matchAll(compRegex)) {
    const block = match[0];
    const nameMatch = block.match(/<Name>([^<]+)<\/Name>/);
    const teamMatch = block.match(/<Team>([^<]+)<\/Team>/);
    const name = nameMatch?.[1]?.trim() || "";
    const team = teamMatch?.[1]?.trim() || "";
    const normName = normalizeRacerName(name);
    const normTeam = normalizeTeamName(team);
    const exact = byNameTeam.get(`${normName}|${normTeam}`);
    const fallbackList = byName.get(normName) || [];
    const fallback = fallbackList.length === 1 ? fallbackList[0] : undefined;
    const entry = exact || fallback;

    if (!entry) {
      unmatchedComps.push({ name, team });
      updatedBlocks.push(block);
      continue;
    }

    used.add(entry.racerId);
    const bib = entry.bib;
    const withBib = block.replace(/<Bib>[^<]*<\/Bib>/, `<Bib>${bib}</Bib>`);
    const withStartNumber = withBib.replace(/<StartNumber>[^<]*<\/StartNumber>/, `<StartNumber>${bib}</StartNumber>`);
    updatedBlocks.push(withStartNumber);
  }

  let idx = 0;
  const updatedXml = xml.replace(compRegex, () => updatedBlocks[idx++] ?? "");
  const unusedEntries = entries.filter(e => !used.has(e.racerId));
  return { updatedXml, unmatchedComps, unusedEntries };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const inputPath = path.resolve(opts.file);
  if (!fs.existsSync(inputPath)) throw new Error(`Input file not found: ${inputPath}`);
  const outputPath = path.resolve(opts.output || opts.file);

  console.log(`Fetching start list for race ${opts.raceId}...`);
  const entries = await fetchStartList(opts);
  if (!entries.length) throw new Error("No start list entries found.");

  console.log(`Loaded ${entries.length} start list entries. Updating ${inputPath}...`);
  const original = fs.readFileSync(inputPath, "utf8");
  const { updatedXml, unmatchedComps, unusedEntries } = updateNatfis(original, entries);
  fs.writeFileSync(outputPath, updatedXml, "utf8");

  console.log(`Updated file written to ${outputPath}`);
  if (unmatchedComps.length) {
    console.warn(`Unmatched competitors (${unmatchedComps.length}):`);
    for (const u of unmatchedComps) console.warn(` - ${u.name} [${u.team}]`);
  }
  if (unusedEntries.length) {
    console.warn(`Start list entries not used (${unusedEntries.length}):`);
    for (const e of unusedEntries) console.warn(` - ${e.racerName} [${e.teamName}] bib ${e.bib}`);
  }
}

main().catch(err => {
  console.error(err);
  process.exitCode = 1;
});
