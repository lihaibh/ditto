import { expect } from "chai";
import { MongoDump } from "./MongoDump";
import * as zlib from "zlib";

describe("MongoDump", () => {
  it("sanity dump", async () => {
    const dump = new MongoDump({
      source: {
        uri: "mongodb://bigid:password@localhot:27017",
        dbname: "bigid-test",
        connectTimeoutMS: 2000,
      },
      target: {
        path: "./hello4.tar",
        gzip: {
          chunkSize: 10000,
          level: zlib.constants.Z_BEST_SPEED,
        },
        cleanupOnFailure: true,
      },
    });

    // dump.on("end", async (summary) => {
    //   console.log(`summary fn: ${summary}`);
    // });

    // const x = 100 + 100;
    // console.log("ended!" + x);

    // expect(true).to.be.equal(false);

    try {
      for await (const event of dump.iterator("progress")) {
        if (event) {
          console.log(event);
        }
      }
    } catch (error) {
      console.error("Error!");
      console.log(error.stack);
    }

    // dump.start();

    // setTimeout(async () => {
    //   const summary = await dump.promise();
    //   console.log("natural summary");
    //   console.log(summary);
    // }, 20);
  });
});
