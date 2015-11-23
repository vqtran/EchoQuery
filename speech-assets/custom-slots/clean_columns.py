def is_ascii(s):
  return all(ord(c) < 128 for c in s)

def clean_columns(filename):
  values = set()
  vals = []
  with open(filename) as f:
    for line in f:
      if is_ascii(line):
        line = line.rstrip()
        if line not in vals:
          vals.append(line) 
  for el in vals:
    print el

clean_columns('COLUMN_VALUE_WORD')
