import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { createAppearanceApi } from "../src/appearance.js";

const api = createAppearanceApi();
const settings = (mode = "auto", dayStart = "07:00", nightStart = "19:00") => ({ mode, dayStart, nightStart });
const at = (hour, minute = 0, second = 0, ms = 0) => new Date(2026, 10, 16, hour, minute, second, ms);
const storage = () => {
  const values = new Map();
  return { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) };
};

test("没有配置时保持白天，默认自动时间为本地 07:00 与 19:00", () => {
  assert.deepEqual(api.read(null), settings("light"));
  assert.deepEqual(api.read(storage()), settings("light"));
  const first = api.defaults();
  first.mode = "dark";
  assert.equal(api.defaults().mode, "light");
});

test("三种模式和自定义时间可保存、重读，忽略无关字段且不修改调用者", () => {
  const saved = storage();
  for (const mode of ["light", "dark", "auto"]) {
    const value = { ...settings(mode, "08:15", "21:45"), unrelated: "future" };
    const result = api.save(saved, value);
    assert.equal(result.ok, true);
    assert.equal(result.persisted, true);
    assert.deepEqual(api.read(saved), settings(mode, "08:15", "21:45"));
    assert.equal(value.unrelated, "future");
    assert.equal(Object.hasOwn(result.settings, "unrelated"), false);
  }
});

test("固定白天和黑夜在全天保持所选模式", () => {
  for (let hour = 0; hour < 24; hour++) {
    assert.equal(api.resolve(settings("light"), at(hour)), "light");
    assert.equal(api.resolve(settings("dark"), at(hour)), "dark");
  }
});

test("自动主题按本地时间判断，白天开始包含边界，黑夜开始切换", () => {
  for (const [hour, minute, expected] of [[0, 0, "dark"], [6, 59, "dark"], [7, 0, "light"], [12, 0, "light"], [18, 59, "light"], [19, 0, "dark"], [23, 59, "dark"]]) {
    assert.equal(api.resolve(settings(), at(hour, minute)), expected, `${hour}:${minute}`);
  }
});

test("自定义白天区间可跨午夜，午夜本身也可作为切换边界", () => {
  for (const [hour, minute, expected] of [[0, 0, "light"], [5, 59, "light"], [6, 0, "dark"], [21, 59, "dark"], [22, 0, "light"], [23, 59, "light"]]) {
    assert.equal(api.resolve(settings("auto", "22:00", "06:00"), at(hour, minute)), expected);
  }
  assert.equal(api.resolve(settings("auto", "07:00", "00:00"), at(23, 59)), "light");
  assert.equal(api.resolve(settings("auto", "07:00", "00:00"), at(0)), "dark");
  assert.equal(api.resolve(settings("auto", "00:00", "06:00"), at(0)), "light");
});

test("非法模式、空时间、越界时间和相等时间均不能保存", () => {
  const saved = storage();
  api.save(saved, settings("dark"));
  for (const value of [null, {}, settings("system"), settings("auto", "", "19:00"), settings("auto", "24:00", "19:00"), settings("auto", "07:60", "19:00"), settings("auto", "7:00", "19:00"), settings("auto", "07:00", "07:00")]) {
    assert.ok(api.validate(value));
    assert.equal(api.save(saved, value).ok, false);
    assert.deepEqual(api.read(saved), settings("dark"));
  }
});

test("损坏、旧版或不完整的存储回到安全默认值", () => {
  const saved = storage();
  for (const value of ["bad json", "null", "[]", JSON.stringify({ version: 2, ...settings("dark") }), JSON.stringify({ version: 1, mode: "dark" }), JSON.stringify({ version: 1, ...settings("auto", "07:00", "07:00") })]) {
    saved.setItem(api.key, value);
    assert.deepEqual(api.read(saved), settings("light"));
  }
  assert.deepEqual(api.read({ getItem() { throw new Error("denied"); } }), settings("light"));
});

test("存储不可用时允许本页应用，但明确报告没有持久化", () => {
  for (const saved of [null, { setItem() { throw new Error("quota"); } }]) {
    const result = api.save(saved, settings("dark"));
    assert.equal(result.ok, true);
    assert.equal(result.persisted, false);
    assert.equal(api.resolve(result.settings, at(12)), "dark");
  }
});

test("自动定时对齐下一分钟，不在同一分钟边界忙循环", () => {
  assert.equal(api.nextDelay(at(7, 0)), 60000);
  assert.equal(api.nextDelay(at(7, 0, 15, 250)), 44750);
  assert.equal(api.nextDelay(at(7, 0, 59, 999)), 1);
  assert.equal(api.nextDelay(new Date(NaN)), 60000);
});

test("首屏工厂序列化后独立运行，不依赖 React 或模块外变量", () => {
  const saved = storage();
  api.save(saved, settings("auto", "22:00", "06:00"));
  const result = vm.runInNewContext(`const api = (${createAppearanceApi.toString()})(); api.resolve(api.read(storage), now);`, { storage: saved, now: at(23) });
  assert.equal(result, "light");
});

test("主题应用同步更新根节点和浏览器配色，缺少可选 meta 也可运行", () => {
  const attributes = new Map();
  const document = { documentElement: { dataset: {} }, querySelector: () => ({ setAttribute: (key, value) => attributes.set(key, value) }) };
  assert.equal(api.apply(document, settings("dark"), at(12)), "dark");
  assert.equal(document.documentElement.dataset.theme, "dark");
  assert.equal(attributes.get("content"), "#101913");
  assert.equal(api.apply(document, settings("light"), at(23)), "light");
  assert.equal(attributes.get("content"), "#f5f7f4");
  document.querySelector = () => null;
  assert.equal(api.apply(document, settings("dark"), at(12)), "dark");
});
