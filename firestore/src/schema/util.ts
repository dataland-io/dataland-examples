import {
  NullValue,
  Value,
  isArrayValue,
  isBooleanValue,
  isBytesValue,
  isDoubleValue,
  isGeoPointValue,
  isIntegerValue,
  isMapValue,
  isNullValue,
  isReferenceValue,
  isStringValue,
  isTimestampValue,
} from "../firestore";

export const isNotNull = <T extends Value>(
  value: T | NullValue | null | undefined
): value is T => {
  return value != null && !isNullValue(value);
};

export const flattenValue = (value: Value): unknown => {
  if (isNullValue(value)) {
    return null;
  } else if (isBooleanValue(value)) {
    return value.booleanValue;
  } else if (isIntegerValue(value)) {
    const parsed = Number(value.integerValue);
    if (parsed == null || !Number.isSafeInteger(parsed)) {
      throw new Error(
        `invalid firestore integer value - ${JSON.stringify(value)}`
      );
    }
    return parsed;
  } else if (isDoubleValue(value)) {
    return value.doubleValue;
  } else if (isTimestampValue(value)) {
    return value.timestampValue;
  } else if (isStringValue(value)) {
    return value.stringValue;
  } else if (isBytesValue(value)) {
    return value.bytesValue;
  } else if (isReferenceValue(value)) {
    return value.referenceValue;
  } else if (isGeoPointValue(value)) {
    return value.geoPointValue;
  } else if (isArrayValue(value)) {
    const flattenedValues: unknown[] = [];
    for (const item of value.arrayValue.values) {
      const flattenedValue = flattenValue(item);
      flattenedValues.push(flattenedValue);
    }
    return flattenedValues;
  } else if (isMapValue(value)) {
    const flattenedValues: Record<string, unknown> = {};
    for (const key in value.mapValue.fields) {
      const item = value.mapValue.fields[key];
      const flattenedValue = flattenValue(item);
      flattenedValues[key] = flattenedValue;
    }
    return flattenedValues;
  } else {
    throw new Error(`invalid firestore value - ${JSON.stringify(value)}`);
  }
};

export type TypeHintFn = (
  value: string | number,
  path: string[]
) => Value | null;

export const unflattenValue = (
  value: unknown,
  typeHintFn: TypeHintFn = () => null,
  currentPath: string[] = []
): Value => {
  if (value == null) {
    return { nullValue: null };
  } else if (typeof value === "boolean") {
    return { booleanValue: value };
  } else if (typeof value === "string") {
    const hint = typeHintFn(value, currentPath);
    if (hint != null) {
      return hint;
    } else {
      return { stringValue: value };
    }
  } else if (typeof value === "number") {
    const hint = typeHintFn(value, currentPath);
    if (hint != null) {
      return hint;
    } else {
      return { doubleValue: value };
    }
  } else if (typeof value === "bigint") {
    return { integerValue: String(value) };
  } else if (typeof value === "object" && value instanceof Date) {
    return { timestampValue: value.toISOString() };
  } else if (Array.isArray(value)) {
    const unflattenedItems: Value[] = [];
    for (let i = 0; i < value.length; i++) {
      const item = value[i];
      const unflattenedItem = unflattenValue(item, typeHintFn, [
        ...currentPath,
        String(i),
      ]);
      unflattenedItems.push(unflattenedItem);
    }
    return { arrayValue: { values: unflattenedItems } };
  } else if (typeof value === "object") {
    const unflattenedItems: Record<string, Value> = {};
    for (const key in value) {
      const item = (value as any)[key];
      const unflattenedItem = unflattenValue(item, typeHintFn, [
        ...currentPath,
        key,
      ]);
      unflattenedItems[key] = unflattenedItem;
    }
    return { mapValue: { fields: unflattenedItems } };
  } else {
    throw new Error(`invalid json value - ${JSON.stringify(value)}`);
  }
};
