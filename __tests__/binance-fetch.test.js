import { describe, it, expect } from "vitest";
import { spawn } from "child_process";
import { join } from "path";
import { Tests } from "./binance-fetch-tests-list.js";
import chokidar from "chokidar";
import fs from "fs/promises";
import { beforeAll } from "vitest";
import { datatypesWithInterval } from "../src/lists.js";

const resultDir = join(import.meta.dirname, "binance-fetch-result");

beforeAll(async () => {
  const files = await fs.readdir(resultDir);
  for (const file of files) {
    if (file !== ".gitignore") {
      await fs.rm(join(resultDir, file), { recursive: true, force: true });
    }
  }
});

describe.concurrent(
  "Ensure that data for each data type of each product is accessed correctly",
  async () => {
    const watcher = chokidar.watch(resultDir, { awaitWriteFinish: false });
    for (const test of Tests) {
      const endpoint = `${test.product}_${test.dataType}_${
        test.date.length === 7 ? "monthly" : "daily"
      }`;
      it(endpoint, async () => {
        let proc;
        function run(dir, product, dataType, date, symbol, interval = "1h") {
          proc = spawn(
            "node",
            [
              join(import.meta.dirname, "..", "bin", "binance-fetch.js"),
              "--date",
              date,
              "--product",
              product,
              "--data-type",
              dataType,
              "--symbols",
              symbol,
              "--intervals",
              interval,
              "--output-path",
              join(resultDir, dir),
            ],
            { killSignal: "SIGKILL" }
          );
        }
        // Build the expected path based on the URL structure
        const byDay = test.date.length > 7;
        let pathParts = ['data'];
        
        // Add product path segment
        if (test.product === 'usd-m') {
          pathParts.push('futures', 'um');
        } else if (test.product === 'coin-m') {
          pathParts.push('futures', 'cm');
        } else {
          pathParts.push(test.product);
        }
        
        // Add daily/monthly segment
        pathParts.push(byDay ? 'daily' : 'monthly');
        
        // Add data type
        pathParts.push(test.dataType);
        
        // Add symbol
        pathParts.push(test.symbol.toUpperCase());
        
        // Add interval if applicable
        if (datatypesWithInterval.includes(test.dataType)) {
          pathParts.push(test.interval);
        }
        
        // Add filename
        const fileName = `${test.symbol.toUpperCase()}-${
          datatypesWithInterval.includes(test.dataType)
            ? test.interval
            : test.dataType
        }-${test.date}(?:_UNVERIFIED)?\\.zip`;
        
        const expectedFilePath = new RegExp(
          join(resultDir, endpoint, ...pathParts, fileName)
        );
        const waitForFile = new Promise((resolve) => {
          watcher.on("change", async (path) => {
            if (expectedFilePath.test(path)) {
              try {
                const file = await fs.readFile(path);
                if (file.length >= 4) {
                  proc.kill();
                  if (
                    /**
                     * verify file signature (should be a ZIP)
                     * zip signature - '\x50\x4b\x03\x04'
                     */
                    Buffer.from([80, 75, 3, 4]).equals(file.subarray(0, 4))
                  ) {
                    resolve(true);
                  } else {
                    resolve(false);
                  }
                }
              } catch {}
            }
          });
        });
        run(endpoint, ...Object.values(test));
        expect(await waitForFile).toBeTruthy();
      });
    }
  }
);
