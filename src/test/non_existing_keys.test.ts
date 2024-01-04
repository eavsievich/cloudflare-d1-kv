import {describe, expect, test} from "vitest";
import {provideTestKv} from "./common";


describe("non existing keys", () => {
  test("get(non existing key) returns empty result", async () => {
    const result = await provideTestKv().get(["abc"]);
    expect(result.key).toStrictEqual(["abc"]);
    expect(result.value).toBeUndefined();
  });

  test("del(non existing key) with {get:true} returns empty result", async () => {
    const result = await provideTestKv().del(["abc"], {get: true});
    expect(result.key).toStrictEqual(["abc"]);
    expect(result.value).toBeUndefined();
  });

  test("list(non existing prefix) returns empty list", async () => {
    const result = await provideTestKv().list({
      prefix: ["abc"],
      limit: 5,
      offset: 0,
    });
    expect(result).toStrictEqual([]);
  });
});
