import fs from 'fs';

/**
 * Atomic file write: write to temp file, then rename.
 * Prevents partial writes if process crashes mid-write.
 */
export function writeFileAtomic(filePath: string, content: string): void {
  const tempPath = `${filePath}.tmp`;
  fs.writeFileSync(tempPath, content);
  fs.renameSync(tempPath, filePath);
}

/**
 * Pad a number to a fixed width with leading zeros or custom character.
 */
export function padNumber(
  num: number,
  width: number = 6,
  pad: string = '0',
): string {
  return num.toString().padStart(width, pad);
}
