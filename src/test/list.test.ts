import {describe, test, expect} from "vitest";
import {createTimeFunction, provideTestKv} from "./common";

async function provideFilledKv(
  timeFunction = createTimeFunction(0),
  total: number = 100,
  notExpired: number = 0,
  expired: number = 0
) {
  const kv = provideTestKv(timeFunction);
  let counter = 0;

  while (notExpired > 0) {
    const c = counter++;
    await kv.set(["foo", "bar", c], c, {ex: 1000});
    notExpired--;
    total--;
  }

  while (expired > 0) {
    const c = counter++;
    await kv.set(["foo", "bar", c], c, {ex: 1});
    expired--;
    total--;
  }

  while (total > 0) {
    const c = counter++;
    await kv.set(["foo", "bar", c], c);
    total--;
  }

  return kv;
}

describe("list tests", () => {
  test("empty kv should return empty list", async () => {
    const kv = provideTestKv();
    const result = await kv.list({
      prefix: ["foo"],
      limit: 5,
      offset: 0,
    });
    expect(result.length).toStrictEqual(0);
  });

  test("kv should return empty list if not common prefix", async () => {
    const kv = await provideFilledKv();
    const result = await kv.list({
      prefix: ["abc"],
      limit: 5,
      offset: 0,
    });
    expect(result.length).toStrictEqual(0);
  });

  test("kv should return all elements with common prefix", async () => {
    const kv = await provideFilledKv(createTimeFunction(0), 100);
    const result = await kv.list({
      prefix: ["foo"],
      limit: 200,
      offset: 0,
    });
    expect(result.length).toStrictEqual(100);
  });

  test("kv should omit expired items", async () => {
    const time = createTimeFunction(0);
    const kv = await provideFilledKv(time, 100, 20, 10);
    time.mockImplementation(() => 10);
    const result = await kv.list({
      prefix: ["foo"],
      limit: 200,
      offset: 0,
    });
    expect(result.length).toStrictEqual(90);
  });

  test("list sorted by key by default", async () => {
    const kv = await provideFilledKv();
    const result = await kv.list({
      prefix: ["foo"],
      limit: 3,
      offset: 0,
    });
    console.log(result);
    expect(result[0].value).toStrictEqual(0);
    expect(result[1].value).toStrictEqual(10);
    expect(result[2].value).toStrictEqual(11);
  });

  test("offset works", async () => {
    const kv = await provideFilledKv();
    const result = await kv.list({
      prefix: ["foo"],
      limit: 3,
      offset: 1,
    });
    console.log(result);
    expect(result[0].value).toStrictEqual(10);
    expect(result[1].value).toStrictEqual(11);
    expect(result[2].value).toStrictEqual(12);
  });

  test("list sort by key reversed works", async () => {
    const kv = await provideFilledKv();
    const result = await kv.list({
      prefix: ["foo"],
      limit: 3,
      offset: 0,
      order: "desc",
    });
    console.log(result);
    expect(result[0].value).toStrictEqual(9);
    expect(result[1].value).toStrictEqual(99);
    expect(result[2].value).toStrictEqual(98);
  });
});