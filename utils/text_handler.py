import re

def clean_content(text):
    # 去除特殊符號與非中文字元
    cleaned_text = re.sub(r'[^一-龥A-Za-z0-9，。、！？；：「」（）\s]', '', text)
    # 去除特殊符號
    cleaned_text = re.sub(r'[^a-zA-Z0-9\u4e00-\u9fff.,!?;:()（）【】「」《》“”‘’]', '', text)
    # 去除多餘空白行
    cleaned_text = re.sub(r'\n+', '\n', cleaned_text).strip()
    return cleaned_text