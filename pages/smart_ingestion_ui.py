import streamlit as st
import pandas as pd
from engines.smart_ingestion import parse_product_links, generate_seo_content, push_to_salla

def render_smart_ingestion():
    st.title("✨ إضافة منتج ذكي (Enterprise)")
    st.markdown("أضف منتجات جديدة لمتجر سلة بذكاء عبر الروابط المباشرة وتوليد SEO آلي.")

    with st.form("smart_add_form"):
        links_text = st.text_area("أدخل روابط المنتجات (رابط في كل سطر)", height=150)
        c1, c2 = st.columns(2)
        generate_seo = c1.checkbox("توليد وصف SEO آلي", value=True)
        auto_push = c2.checkbox("إرسال مباشر لسلة (Make)", value=False)
        
        submit = st.form_submit_button("🚀 تحليل وإضافة", type="primary")

    if submit and links_text:
        links = [l.strip() for l in links_text.split("\n") if l.strip()]
        with st.spinner(f"جاري تحليل {len(links)} رابط..."):
            results = parse_product_links(links)
            st.session_state.ingestion_results = results
            st.success(f"✅ تم تحليل {len(results)} منتج بنجاح")

    if "ingestion_results" in st.session_state:
        st.subheader("📦 المنتجات المكتشفة")
        df = pd.DataFrame(st.session_state.ingestion_results)
        st.dataframe(df, use_container_width=True)
        
        if st.button("📤 إرسال الكل لسلة"):
            for item in st.session_state.ingestion_results:
                # محاكاة الإرسال
                push_to_salla(item)
            st.success("✅ تم إرسال المنتجات بنجاح")

if __name__ == "__main__":
    render_smart_ingestion()
