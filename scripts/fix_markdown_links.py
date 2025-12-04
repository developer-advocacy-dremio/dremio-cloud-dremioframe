import os
import re
from pathlib import Path

def fix_markdown_links(directory):
    """
    Recursively find all markdown files and remove incompatible permalinks.
    1. Remove: [​](#anchor "Direct link to ...")
    2. Replace: [Text](#anchor) with Text
    """
    directory_path = Path(directory)
    
    if not directory_path.exists():
        print(f"Directory '{directory}' does not exist")
        return

    # Regex 1: Match the specific "Direct link" pattern (often has zero-width space)
    # Matches [anything](#anything "Direct link to anything")
    pattern_direct_link = re.compile(r'\[.*?\]\(#[^)]+ "Direct link to .*?"\)')
    
    # Regex 2: Match internal links [Text](#anchor)
    # We want to keep 'Text' but remove the link part.
    pattern_internal_link = re.compile(r'\[([^\]]+)\]\(#[^)]+\)')
    
    markdown_files = sorted(directory_path.rglob("*.md"))
    print(f"Found {len(markdown_files)} markdown files to process.")
    
    files_modified = 0
    
    for md_file in markdown_files:
        try:
            with open(md_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # First pass: remove the "Direct link" artifacts completely
            new_content = pattern_direct_link.sub('', content)
            
            # Second pass: replace [Text](#anchor) with Text
            new_content = pattern_internal_link.sub(r'\1', new_content)
            
            if content != new_content:
                with open(md_file, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                print(f"Fixed links in: {md_file}")
                files_modified += 1
                
        except Exception as e:
            print(f"Error processing {md_file}: {e}")
            
    print(f"Finished processing. Modified {files_modified} files.")

if __name__ == "__main__":
    # Target the dremiodocs folder relative to the script or CWD
    target_dir = "/home/alexmerced/development/personal/Personal/2026/dremioframe/dremiodocs"
    fix_markdown_links(target_dir)
