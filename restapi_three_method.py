from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from config import username, password, db_name
import pandas as pd

# Создание приложения FastAPI
app = FastAPI()

connection_string = f"postgresql://{username}:{password}@localhost/{db_name}"

# Подключение к базе данных PostgreSQL
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)
session = Session()

# Создание базовой модели для использования в SQL-алхимии
Base = declarative_base()


# Описание моделей таблиц БД
class FileVersion(Base):
    __tablename__ = 'file_versions'

    id = Column(Integer, primary_key=True)
    version = Column(String, unique=True)
    file_name = Column(String)


class Project(Base):
    __tablename__ = 'projects'

    id = Column(Integer, primary_key=True)
    code = Column(Integer, unique=True)
    name = Column(String(100))


class Value(Base):
    __tablename__ = 'values'

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('projects.id'))
    file_version_id = Column(Integer, ForeignKey('file_versions.id'))
    date = Column(Date)
    plan = Column(Integer)
    fact = Column(Integer)


# Создание таблиц в базе данных (DDL)
Base.metadata.create_all(bind=engine)


# Модели данных для запросов API
class FileVersionCreate(BaseModel):
    version: str
    file_name: str


class FileVersionResponse(BaseModel):
    id: int
    version: str
    file_name: str


class ValueCreate(BaseModel):
    project_id: int
    file_version_id: int
    date: str
    plan: int
    fact: int


class ChartDataResponse(BaseModel):
    data: dict


# Метод загрузки файла
@app.post('/files', response_model=FileVersionResponse)
def create_file_version(file_version: FileVersionCreate):
    # Создание новой версии файла
    db_file_version = FileVersion(version=file_version.version, file_name=file_version.file_name)
    session.add(db_file_version)
    session.commit()
    session.refresh(db_file_version)
    return db_file_version


# Метод загрузки значений данных
@app.post('/values')
def create_value(value: ValueCreate):
    # Поиск проекта по коду
    db_project = session.query(Project).filter(Project.id == value.project_id).first()

    # Поиск версии файла по идентификатору
    db_file_version = session.query(FileVersion).filter(FileVersion.id == value.file_version_id).first()

    # Создание нового значения данных
    db_value = Value(
        project_id=db_project.id,
        file_version_id=db_file_version.id,
        date=value.date,
        plan=value.plan,
        fact=value.fact
    )

    session.add(db_value)
    session.commit()
    session.refresh(db_value)
    return {'success': True}


# Метод для получения данных для столбчатой диаграммы
@app.get('/chart-data')
def get_chart_data(version: str, year: int, value_type: str):
    # Поиск версии файла по указанной версии
    db_file_version = session.query(FileVersion).filter(FileVersion.version == version).first()

    # Поиск всех значений по указанной версии и году
    db_values = session.query(Value).join(Project).\
        filter(FileVersion.id == db_file_version.id).\
        filter(Value.date >= f'{year}-01-01').\
        filter(Value.date <= f'{year}-12-31').all()

    # Подсчет суммарных значений для каждой даты
    data = {}
    for value in db_values:
        date_str = value.date.strftime("%Y-%m-%d")
        if date_str not in data:
            data[date_str] = 0

        if value_type == 'plan':
            data[date_str] += value.plan
        elif value_type == 'fact':
            data[date_str] += value.fact

    return {'data': data}


# Загрузка данных из файла example.xlsx и сохранение в базе данных
def load_data_from_excel():
    df = pd.read_excel('example.xlsx')

    for row in df.itertuples():
        project_code = row[1]
        version = row[2]
        date = row[3]
        plan_value = row[4]
        fact_value = row[5]

        # Поиск проекта по коду
        db_project = session.query(Project).filter(Project.code == project_code).first()

        # Поиск версии файла по указанной версии
        db_file_version = session.query(FileVersion).filter(FileVersion.version == version).first()

        # Создание нового значения данных
        db_value = Value(
            project_id=db_project.id,
            file_version_id=db_file_version.id,
            date=date,
            plan=plan_value,
            fact=fact_value
        )

        session.add(db_value)

    session.commit()




if __name__ == '__main__':
    import uvicorn

    load_data_from_excel()

    uvicorn.run(app, host='0.0.0.0', port=8000)