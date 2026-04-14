import re

def convert_fstrings_to_format(file_path):
    """
    Reads a Python file and converts f-strings like f"{a} {b}" 
    to old-style "{} {}".format(a, b)
    
    Args:
        file_path: Path to the Python file to convert
    """
    
    # Specify UTF-8 encoding explicitly
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Pattern to match f-strings
    # Matches f"..." or f'...' with capture groups for the quoted content
    pattern = r'f(["\'])(.*?)\1'
    
    def replacement(match):
        quote_char = match.group(1)  # Either " or '
        string_content = match.group(2)  # Content inside the quotes
        
        # Find all expressions inside {} in the string
        expr_pattern = r'\{([^{}]+)\}'
        expressions = re.findall(expr_pattern, string_content)
        
        if not expressions:
            # No expressions, just return regular string without f prefix
            return f'{quote_char}{string_content}{quote_char}'
        
        # Replace {expr} with {} for .format()
        new_string = re.sub(expr_pattern, '{}', string_content)
        
        # Build the .format() arguments
        format_args = ', '.join(expressions)
        
        # Return the converted version
        return f'{quote_char}{new_string}{quote_char}.format({format_args})'
    
    # Replace all f-strings
    converted_content = re.sub(pattern, replacement, content, flags=re.DOTALL)
    
    # Write back to file with UTF-8 encoding
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(converted_content)
    
    print(f"Converted f-strings in {file_path}")


# Example usage:
if __name__ == "__main__":
    convert_fstrings_to_format("check-tables.py")