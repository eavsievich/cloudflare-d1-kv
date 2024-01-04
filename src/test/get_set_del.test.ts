// noinspection DuplicatedCode

import {describe, expect, test} from "vitest";
import {createTimeFunction, provideTestKv} from "./common";

const key = ["key1", "key2", 1];

describe("get set del tests", () => {
  test("basic set-get", async () => {
    const timeFunction = createTimeFunction(13);
    const kv = provideTestKv(timeFunction);
    await kv.set(key, 1);
    const result = await kv.get<number>(key);
    expect(result.key).toStrictEqual(key);
    expect(result.value).toStrictEqual(1);
    expect(result.createdAt).toStrictEqual(13);
    expect(result.updatedAt).toStrictEqual(13);
    expect(result.expiresAt).toStrictEqual(-1);
  });

  test("set with ex", async () => {
    const timeFunction = createTimeFunction(13);
    const kv = provideTestKv(timeFunction);
    await kv.set(key, 1, {ex: 10});
    const result = await kv.get<number>(key);
    expect(result.key).toStrictEqual(key);
    expect(result.value).toStrictEqual(1);
    expect(result.createdAt).toStrictEqual(13);
    expect(result.updatedAt).toStrictEqual(13);
    expect(result.expiresAt).toStrictEqual(23);
  });

  test("expired should return empty result", async () => {
    const timeFunction = createTimeFunction(0);
    const kv = provideTestKv(timeFunction);
    await kv.set(key, 1, {ex: 5});
    timeFunction.mockImplementation(() => 10);
    const result = await kv.get<number>(key);
    expect(result.key).toStrictEqual(key);
    expect(result.value).toBeUndefined();
    expect(result.createdAt).toBeUndefined();
    expect(result.updatedAt).toBeUndefined();
    expect(result.expiresAt).toBeUndefined();
  });

  test("set should return empty without get", async () => {
    const timeFunction = createTimeFunction(13);
    const kv = provideTestKv(timeFunction);
    const result = await kv.set(key, 1);
    expect(result.key).toStrictEqual(key);
    expect(result.value).toBeUndefined();
    expect(result.createdAt).toBeUndefined();
    expect(result.updatedAt).toBeUndefined();
    expect(result.expiresAt).toBeUndefined();
  });

  test("set should return empty with get for the first time", async () => {
    const timeFunction = createTimeFunction(13);
    const kv = provideTestKv(timeFunction);
    const result = await kv.set(key, 1, {get: true});
    expect(result.key).toStrictEqual(key);
    expect(result.value).toBeUndefined();
    expect(result.createdAt).toBeUndefined();
    expect(result.updatedAt).toBeUndefined();
    expect(result.expiresAt).toBeUndefined();
  });

  test("set should return previous value with get", async () => {
    const timeFunction = createTimeFunction(13);
    const kv = provideTestKv(timeFunction);
    await kv.set(key, 1);
    const result = await kv.set(key, 2, {get: true});
    expect(result.key).toStrictEqual(key);
    expect(result.value).toStrictEqual(1);
    expect(result.createdAt).toStrictEqual(13);
    expect(result.updatedAt).toStrictEqual(13);
    expect(result.expiresAt).toStrictEqual(-1);
  });

  test("updated_at should be updated at overwritten", async () => {
    const timeFunction = createTimeFunction(13);
    const kv = provideTestKv(timeFunction);
    await kv.set(key, 1);
    timeFunction.mockImplementation(() => 14);
    await kv.set(key, 2);
    const result = await kv.get<number>(key);
    expect(result.key).toStrictEqual(key);
    expect(result.value).toStrictEqual(2);
    expect(result.createdAt).toStrictEqual(13);
    expect(result.updatedAt).toStrictEqual(14);
    expect(result.expiresAt).toStrictEqual(-1);
  });

  test("expires_at should be updated at overwritten", async () => {
    const timeFunction = createTimeFunction(13);
    const kv = provideTestKv(timeFunction);
    await kv.set(key, 1);
    const result0 = await kv.get<number>(key);
    expect(result0.expiresAt).toStrictEqual(-1);

    timeFunction.mockImplementation(() => 14);
    await kv.set(key, 2, {ex: 10});
    const result = await kv.get<number>(key);
    expect(result.key).toStrictEqual(key);
    expect(result.value).toStrictEqual(2);
    expect(result.createdAt).toStrictEqual(13);
    expect(result.updatedAt).toStrictEqual(14);
    expect(result.expiresAt).toStrictEqual(24);
  });

  test("set with nx should work if key is not exists", async () => {
    const timeFunction = createTimeFunction(13);
    const kv = provideTestKv(timeFunction);
    await kv.set(key, 1, {nx: true});
    const result = await kv.get<number>(key);
    expect(result.key).toStrictEqual(key);
    expect(result.value).toStrictEqual(1);
    expect(result.createdAt).toStrictEqual(13);
    expect(result.updatedAt).toStrictEqual(13);
    expect(result.expiresAt).toStrictEqual(-1);
  });

  test("set with nx should do nothing if key exists", async () => {
    const timeFunction = createTimeFunction(13);
    const kv = provideTestKv(timeFunction);
    await kv.set(key, 1);
    await kv.set(key, 2, {nx: true});
    const result = await kv.get<number>(key);
    expect(result.key).toStrictEqual(key);
    expect(result.value).toStrictEqual(1);
    expect(result.createdAt).toStrictEqual(13);
    expect(result.updatedAt).toStrictEqual(13);
    expect(result.expiresAt).toStrictEqual(-1);
  });

  test("delete works", async () => {
    const timeFunction = createTimeFunction(13);
    const kv = provideTestKv(timeFunction);
    await kv.set(key, 1);

    const result0 = await kv.get(key);
    expect(result0.key).toStrictEqual(key);
    expect(result0.value).toStrictEqual(1);

    const delResult = await kv.del(key);
    expect(delResult.key).toStrictEqual(key);
    expect(delResult.value).toBeUndefined();

    const result1 = await kv.get(key);
    expect(result1.key).toStrictEqual(key);
    expect(result1.value).toBeUndefined();
  });

  test("delete return previous value with get", async () => {
    const timeFunction = createTimeFunction(13);
    const kv = provideTestKv(timeFunction);
    await kv.set(key, 1);

    const result0 = await kv.get(key);
    expect(result0.key).toStrictEqual(key);
    expect(result0.value).toStrictEqual(1);

    const delResult = await kv.del(key, {get: true});
    expect(delResult.key).toStrictEqual(key);
    expect(delResult.value).toStrictEqual(1);

    const result1 = await kv.get(key);
    expect(result1.key).toStrictEqual(key);
    expect(result1.value).toBeUndefined();
  });
});
