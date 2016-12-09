export function truncate(string, value) {
    value = value || 200;

    return string.length <= value ? string : string.substring(0, value) + "...";
}

export function capitalize(value) {
    if (!value) return "";
    value = value.toString();
    
    return value.charAt(0).toUpperCase() + value.slice(1);
}

export function pluralize(amount, label) {
  if (amount === 1) {
    return amount + label
  }
  
  return amount + label + 's'
}

export function title(str)
{
  str = str.replace("_", " ");
  return str.replace(/\b\w+/g, txt => txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase());
}