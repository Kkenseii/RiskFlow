import streamlit as st
import pandas as pd
from orchestrator import Orchestrator

st.set_page_config(page_title="RiskFlow", page_icon="⚙️", layout="wide")
st.title("⚙️ RiskFlow — оркестратор оценки рисков")
st.caption("Учебный MVP: очередь, конкурентный захват ресурсов, история запусков")

@st.cache_resource
def get_orchestrator():
    return Orchestrator()
engine=get_orchestrator()

with st.sidebar:
    st.header("Новая задача")
    process=st.selectbox("Бизнес-процесс",["Кредитный риск","Рыночный риск","Операционный риск"])
    business_date=st.date_input("Дата расчёта")
    sources=st.multiselect("Источники данных",
        ["warehouse_credit","market_data","client_registry","transactions","operational_events"],
        default=["warehouse_credit"])
    user=st.text_input("Пользователь", "Демонстрационный пользователь")
    trigger=st.selectbox("Тип запуска",["manual","scheduled"])
    if st.button("Добавить в очередь",use_container_width=True):
        try:
            task_id=engine.submit(process,str(business_date),sources,user,trigger)
            st.success(f"Задача {task_id} добавлена")
        except ValueError as e:
            st.error(str(e))
    if st.button("Запустить обработчик",use_container_width=True):
        engine.start()
        st.success("Обработчик запущен")

tasks=engine.list_tasks()
c1,c2,c3,c4=st.columns(4)
c1.metric("Всего задач",len(tasks))
c2.metric("В очереди",sum(t["status"]=="queued" for t in tasks))
c3.metric("Выполняется",sum(t["status"]=="running" for t in tasks))
c4.metric("Завершено",sum(t["status"]=="completed" for t in tasks))

locks=engine.list_locks()
st.caption(f"Активных блокировок: {len(locks)} · Обработчик запускается кнопкой в боковой панели")

st.subheader("Мониторинг процессов")
if tasks:
    df=pd.DataFrame([{
        "ID":t["id"],"Процесс":t["process"],"Дата":t["business_date"],
        "Источники":", ".join(t["source_keys"]),"Инициатор":t["requested_by"],
        "Тип":t["trigger"],"Статус":t["status"],"Сообщение":t["message"] or ""
    } for t in tasks])
    st.dataframe(df,use_container_width=True,hide_index=True)
else:
    st.info("Задач пока нет. Добавьте задачу в боковой панели.")
with st.expander("Активные блокировки"):
    if locks:
        st.dataframe(pd.DataFrame(locks), use_container_width=True, hide_index=True)
    else:
        st.info("Активных блокировок нет.")

if st.button("Обновить данные"):
    st.rerun()
st.caption("Статусы: queued — очередь; running — выполняется; completed — завершено; failed — ошибка. Одинаковый процесс, дата и набор источников объединяются в одну задачу.")
