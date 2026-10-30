const pins = ['artifactFileSha256', 'instanceFileSha256', 'portfolioFileSha256'];
export function restoreOperatingProfile(profile, catalog) {
  if (!profile || profile.schemaVersion !== 'operating-profile-v1' ||
      ![5, 6, 7].includes(profile.durationHours) || !Array.isArray(profile.selections) ||
      !profile.selections.length || !Array.isArray(catalog)) return null;
  const selection = [];
  for (const saved of profile.selections) {
    const matches = catalog.filter(row => row.selectionKey === saved?.selectionKey);
    if (matches.length !== 1 || selection.includes(saved.selectionKey)) return null;
    const row = matches[0];
    if (!pins.some(key => /^[a-f0-9]{64}$/i.test(saved[key] || '')) ||
        !pins.every(key => (row[key] || '') === (saved[key] || ''))) return null;
    selection.push(row.selectionKey);
  }
  return {selection, durationHours:profile.durationHours, futuresPolicy:profile.futuresPolicy};
}
