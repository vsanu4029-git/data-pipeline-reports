escape_characters=['\n', '\t', '\r', '\\', '\b','\f', '\v','\"',"\'",'\0','\a','\x1b', '""']

def replace_excape_characters(line, esc_char):
    for char in esc_char:
        line=line.replace(char, '?')
        return line
    

orginal_file= r"C:\Users\BIRSTSA\Desktop\stg_items.txt"
cleaned_file = r"C:\Users\BIRSTSA\Desktop\cleaned_file.txt"

with open(orginal_file, 'r') as orgfile, open(cleaned_file, 'w', newline='') as cleanfile:
    for line in orgfile:
        clean_line = replace_excape_characters(line, escape_characters)
        cleanfile.write(clean_line)

print(f"escape characters replaced with ? in  {cleaned_file}")
    