from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
import datetime
import uvicorn
import uuid
from sqlalchemy.dialects.sqlite import VARCHAR

# --- НАСТРОЙКИ БД ---
DATABASE_URL = "sqlite:///./taxi_fleet.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# --- КАСТОМНЫЙ ТИП UUID ДЛЯ SQLITE ---
class UUIDType(VARCHAR):
    """Кастомный тип UUID для SQLite"""

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            elif isinstance(value, uuid.UUID):
                return str(value)
            else:
                return str(value)

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            else:
                return uuid.UUID(value)

        return process


# --- МОДЕЛИ БД (Таблицы) ---

class CarStatus(Base):
    __tablename__ = "car_statuses"

    id = Column(Integer, primary_key=True)
    status_name = Column(String, unique=True, nullable=False)

    # Связь с машинами
    cars = relationship("Car", back_populates="status_rel")


class Driver(Base):
    __tablename__ = "drivers"

    id = Column(UUIDType(36), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    phone = Column(String, unique=True, nullable=False)  # Добавлено поле телефона
    rating = Column(Float, default=5.0)

    # Внешний ключ на машину
    car_id = Column(Integer, ForeignKey("cars.id"), nullable=True)

    # Связь с машиной
    car = relationship("Car", foreign_keys=[car_id])


class Car(Base):
    __tablename__ = "cars"

    id = Column(Integer, primary_key=True, index=True)
    number = Column(String, unique=True, nullable=False)
    brand = Column(String, nullable=False)
    color = Column(String, nullable=False)
    distance_to_caller = Column(Float, default=0.0)

    # Внешний ключ на статус
    status_id = Column(Integer, ForeignKey("car_statuses.id"), nullable=False)

    # Внешний ключ на водителя
    driver_id = Column(UUIDType(36), ForeignKey("drivers.id"), nullable=True)

    # Связи
    status_rel = relationship("CarStatus", back_populates="cars")
    driver = relationship("Driver", foreign_keys=[driver_id])


# --- СОЗДАНИЕ ТАБЛИЦ ---
Base.metadata.create_all(bind=engine)


# --- SCHEMAS (Pydantic для валидации) ---

class DriverBase(BaseModel):
    name: str
    phone: str  # Добавлено поле телефона
    rating: Optional[float] = 5.0


class DriverCreate(DriverBase):
    pass


class DriverResponse(DriverBase):
    id: str  # UUID будет строкой в ответе
    current_car: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True
        # Преобразуем UUID в строку при сериализации
        json_encoders = {
            uuid.UUID: str
        }


class CarBase(BaseModel):
    number: str
    brand: str
    color: str
    distance_to_caller: Optional[float] = 0.0


class CarCreate(CarBase):
    status: str = "Свободна"
    driver_id: Optional[str] = None  # UUID как строка


class CarResponse(BaseModel):
    id: int
    number: str
    brand: str
    color: str
    distance_to_caller: float
    driver: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True


class ErrorResponse(BaseModel):
    timestamp: str
    status: int
    error: str
    message: str


# --- АВТОРИЗАЦИЯ (как в примере) ---
app = FastAPI(title="Taxi Fleet Admin API")
security = HTTPBearer()


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != "secret_token_123":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
                "status": 401,
                "error": "Unauthorized",
                "message": "Требуется действительный JWT токен администратора"
            }
        )
    return credentials.credentials


# --- ЗАВИСИМОСТЬ ДЛЯ БД ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --- ФУНКЦИЯ ДЛЯ ПОЛУЧЕНИЯ СТАТУСА ID ПО НАЗВАНИЮ ---
def get_status_id(db: Session, status_name: str) -> int:
    status = db.query(CarStatus).filter(CarStatus.status_name == status_name).first()
    if not status:
        raise HTTPException(status_code=404, detail=f"Статус '{status_name}' не найден")
    return status.id


# --- ФУНКЦИЯ ДЛЯ ПАРСИНГА UUID ---
def parse_uuid(uuid_str: str) -> uuid.UUID:
    try:
        return uuid.UUID(uuid_str)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Неверный формат UUID: {uuid_str}")


# --- НАПОЛНЕНИЕ ДАННЫМИ (5-6 записей) ---
@app.on_event("startup")
def seed_data():
    db = SessionLocal()
    try:
        # Создаем статусы (3 записи)
        if db.query(CarStatus).count() == 0:
            statuses = [
                CarStatus(id=1, status_name="Свободна"),
                CarStatus(id=2, status_name="В поездке"),
                CarStatus(id=3, status_name="В ремонте")
            ]
            db.add_all(statuses)
            db.commit()
            print("✅ Статусы созданы")

        # Генерируем UUID для водителей
        driver1_id = uuid.uuid4()
        driver2_id = uuid.uuid4()
        driver3_id = uuid.uuid4()
        driver4_id = uuid.uuid4()
        driver5_id = uuid.uuid4()
        driver6_id = uuid.uuid4()

        # Создаем машины СНАЧАЛА (без водителей)
        if db.query(Car).count() == 0:
            cars = [
                Car(
                    id=1,
                    number="ТВ101К174",
                    brand="Toyota",
                    color="Белый",
                    distance_to_caller=0.5,
                    status_id=2,  # В поездке
                    driver_id=driver1_id
                ),
                Car(
                    id=2,
                    number="А123ВС777",
                    brand="Hyundai",
                    color="Черный",
                    distance_to_caller=2.1,
                    status_id=1,  # Свободна
                    driver_id=None
                ),
                Car(
                    id=3,
                    number="В456ОР178",
                    brand="Toyota",
                    color="Синий",
                    distance_to_caller=1.2,
                    status_id=2,  # В поездке
                    driver_id=driver2_id
                ),
                Car(
                    id=4,
                    number="К789МН178",
                    brand="Kia",
                    color="Белый",
                    distance_to_caller=3.0,
                    status_id=3,  # В ремонте
                    driver_id=None
                ),
                Car(
                    id=5,
                    number="Т555АА178",
                    brand="BMW",
                    color="Черный",
                    distance_to_caller=4.5,
                    status_id=2,  # В поездке
                    driver_id=driver3_id
                ),
                Car(
                    id=6,
                    number="Х999ХХ777",
                    brand="Kia",
                    color="Синий",
                    distance_to_caller=0.0,
                    status_id=1,  # Свободна
                    driver_id=None
                )
            ]
            db.add_all(cars)
            db.commit()
            print("✅ Машины созданы")

        # Создаем водителей ПОТОМ
        if db.query(Driver).count() == 0:
            drivers = [
                Driver(id=driver1_id, name="Иванов Иван", phone="+79991234567", rating=4.8, car_id=1),
                Driver(id=driver2_id, name="Петров Петр", phone="+79992345678", rating=4.6, car_id=3),
                Driver(id=driver3_id, name="Сидоров Сидор", phone="+79993456789", rating=4.2, car_id=5),
                Driver(id=driver4_id, name="Смирнов Алексей", phone="+79994567890", rating=4.9, car_id=None),
                Driver(id=driver5_id, name="Кузнецов Дмитрий", phone="+79995678901", rating=3.8, car_id=None),
                Driver(id=driver6_id, name="Попов Андрей", phone="+79996789012", rating=2.9, car_id=None)
            ]
            db.add_all(drivers)
            db.commit()
            print("✅ Водители созданы")

            print("✅ Связи между машинами и водителями установлены")

    except Exception as e:
        print(f"❌ Ошибка при заполнении данных: {e}")
        db.rollback()
    finally:
        db.close()


# --- ЭНДПОИНТЫ ---

@app.get("/", tags=["Root"])
def read_root():
    return {"message": "Taxi Fleet API is live. Go to /docs for Admin Panel."}


# 1. Получить список машин по статусу
@app.get(
    "/api/cars/status/{status}",
    dependencies=[Depends(verify_token)],
    responses={401: {"model": ErrorResponse}}
)
def get_cars_by_status(
        status: str,
        brand: Optional[str] = None,
        color: Optional[str] = None,
        db: Session = Depends(get_db)
):
    # Получаем ID статуса
    status_id = get_status_id(db, status)

    # Формируем запрос
    query = db.query(Car).filter(Car.status_id == status_id)

    if brand:
        query = query.filter(Car.brand.ilike(f"%{brand}%"))
    if color:
        query = query.filter(Car.color.ilike(f"%{color}%"))

    cars = query.all()

    result = []
    for car in cars:
        car_data = {
            "id": car.id,
            "number": car.number,
            "brand": car.brand,
            "color": car.color,
            "distanceToCaller": car.distance_to_caller,
            "driver": None
        }

        if car.driver_id:
            driver = db.query(Driver).filter(Driver.id == car.driver_id).first()
            if driver:
                car_data["driver"] = {
                    "id": str(driver.id),  # UUID в строку
                    "name": driver.name,
                    "phone": driver.phone,  # Добавлен телефон
                    "rating": driver.rating
                }

        result.append(car_data)

    return {
        "count": len(result),
        "cars": result
    }


# 2. Получить список всех водителей
@app.get(
    "/api/drivers",
    dependencies=[Depends(verify_token)],
    responses={401: {"model": ErrorResponse}}
)
def get_drivers(
        minRating: Optional[float] = None,
        maxRating: Optional[float] = None,
        db: Session = Depends(get_db)
):
    query = db.query(Driver)

    if minRating is not None:
        query = query.filter(Driver.rating >= minRating)
    if maxRating is not None:
        query = query.filter(Driver.rating <= maxRating)

    drivers = query.all()

    result = []
    for driver in drivers:
        driver_data = {
            "id": str(driver.id),  # UUID в строку
            "name": driver.name,
            "phone": driver.phone,  # Добавлен телефон
            "rating": driver.rating,
            "currentCar": None
        }

        if driver.car_id:
            car = db.query(Car).filter(Car.id == driver.car_id).first()
            if car:
                driver_data["currentCar"] = {
                    "id": car.id,
                    "number": car.number,
                    "brand": car.brand
                }

        result.append(driver_data)

    return {
        "count": len(result),
        "drivers": result
    }


# 3. Получить список всех машин по свойствам
@app.get(
    "/api/cars",
    dependencies=[Depends(verify_token)],
    responses={401: {"model": ErrorResponse}}
)
def get_cars(
        status: Optional[str] = None,
        brand: Optional[str] = None,
        color: Optional[str] = None,
        db: Session = Depends(get_db)
):
    query = db.query(Car)

    if status:
        status_id = get_status_id(db, status)
        query = query.filter(Car.status_id == status_id)
    if brand:
        query = query.filter(Car.brand.ilike(f"%{brand}%"))
    if color:
        query = query.filter(Car.color.ilike(f"%{color}%"))

    cars = query.all()

    result = []
    for car in cars:
        status_obj = db.query(CarStatus).filter(CarStatus.id == car.status_id).first()

        car_data = {
            "id": car.id,
            "number": car.number,
            "brand": car.brand,
            "color": car.color,
            "status": status_obj.status_name if status_obj else "unknown",
            "distanceToCaller": car.distance_to_caller,
            "driver": None
        }

        if car.driver_id:
            driver = db.query(Driver).filter(Driver.id == car.driver_id).first()
            if driver:
                car_data["driver"] = {
                    "id": str(driver.id),  # UUID в строку
                    "name": driver.name,
                    "phone": driver.phone,  # Добавлен телефон
                    "rating": driver.rating
                }

        result.append(car_data)

    return {
        "totalCount": len(result),
        "currentPage": 1,
        "cars": result
    }


# 4. Создать новую машину
@app.post(
    "/api/cars",
    dependencies=[Depends(verify_token)],
    status_code=status.HTTP_201_CREATED,
    responses={401: {"model": ErrorResponse}}
)
def create_car(car: CarCreate, db: Session = Depends(get_db)):
    # Проверяем уникальность номера
    existing = db.query(Car).filter(Car.number == car.number).first()
    if existing:
        raise HTTPException(status_code=400, detail="Машина с таким номером уже существует")

    # Получаем ID статуса
    status_id = get_status_id(db, car.status)

    # Парсим UUID водителя, если он указан
    driver_uuid = None
    if car.driver_id:
        driver_uuid = parse_uuid(car.driver_id)
        # Проверяем, что водитель существует
        driver = db.query(Driver).filter(Driver.id == driver_uuid).first()
        if not driver:
            raise HTTPException(status_code=404, detail="Водитель не найден")

    # Создаем машину
    db_car = Car(
        number=car.number,
        brand=car.brand,
        color=car.color,
        distance_to_caller=car.distance_to_caller,
        status_id=status_id,
        driver_id=driver_uuid
    )

    db.add(db_car)
    db.commit()
    db.refresh(db_car)

    # Если указан водитель - обновляем его car_id
    if driver_uuid:
        driver = db.query(Driver).filter(Driver.id == driver_uuid).first()
        if driver:
            driver.car_id = db_car.id
            db.commit()

    # Формируем ответ
    status_obj = db.query(CarStatus).filter(CarStatus.id == db_car.status_id).first()

    response = {
        "id": db_car.id,
        "number": db_car.number,
        "brand": db_car.brand,
        "color": db_car.color,
        "status": status_obj.status_name if status_obj else car.status,
        "driver": None,
        "distanceToCaller": db_car.distance_to_caller,
        "createdAt": datetime.datetime.utcnow().isoformat() + "Z"
    }

    if db_car.driver_id:
        driver = db.query(Driver).filter(Driver.id == db_car.driver_id).first()
        if driver:
            response["driver"] = {
                "id": str(driver.id),  # UUID в строку
                "name": driver.name,
                "phone": driver.phone,
                "rating": driver.rating
            }

    return {
        "message": "Машина успешно добавлена в автопарк",
        "car": response
    }


# 5. Создать нового водителя
@app.post(
    "/api/drivers",
    dependencies=[Depends(verify_token)],
    status_code=status.HTTP_201_CREATED,
    responses={401: {"model": ErrorResponse}}
)
def create_driver(driver: DriverCreate, db: Session = Depends(get_db)):
    # Проверяем уникальность телефона
    existing = db.query(Driver).filter(Driver.phone == driver.phone).first()
    if existing:
        raise HTTPException(status_code=400, detail="Водитель с таким телефоном уже существует")

    # Генерируем новый UUID
    driver_id = uuid.uuid4()

    db_driver = Driver(
        id=driver_id,
        name=driver.name,
        phone=driver.phone,
        rating=driver.rating
    )

    db.add(db_driver)
    db.commit()
    db.refresh(db_driver)

    return {
        "message": "Водитель успешно добавлен",
        "driver": {
            "id": str(db_driver.id),  # UUID в строку
            "name": db_driver.name,
            "phone": db_driver.phone,
            "rating": db_driver.rating
        }
    }


# 6. Обновить расстояние до машины
@app.put(
    "/api/car/distance",
    dependencies=[Depends(verify_token)],
    responses={401: {"model": ErrorResponse}}
)
def update_car_distance(
        car_id: int,
        distance_to_caller: float,
        db: Session = Depends(get_db)
):
    car = db.query(Car).filter(Car.id == car_id).first()
    if not car:
        raise HTTPException(status_code=404, detail="Машина не найдена")

    car.distance_to_caller = distance_to_caller
    db.commit()

    return {
        "status": "OK",
        "message": "Расстояние обновлено успешно."
    }


# 7. Привязать водителя к машине
@app.post(
    "/api/car/bind-driver",
    dependencies=[Depends(verify_token)],
    responses={401: {"model": ErrorResponse}}
)
def bind_driver_to_car(
        driver_id: str,
        car_id: int,
        db: Session = Depends(get_db)
):
    # Парсим UUID водителя
    driver_uuid = parse_uuid(driver_id)

    # Проверяем существование водителя
    driver = db.query(Driver).filter(Driver.id == driver_uuid).first()
    if not driver:
        raise HTTPException(status_code=404, detail="Водитель не найден")

    # Проверяем существование машины
    car = db.query(Car).filter(Car.id == car_id).first()
    if not car:
        raise HTTPException(status_code=404, detail="Машина не найдена")

    # Если у водителя уже есть машина - отвязываем
    if driver.car_id:
        old_car = db.query(Car).filter(Car.id == driver.car_id).first()
        if old_car:
            old_car.driver_id = None

    # Если у машины уже есть водитель - отвязываем
    if car.driver_id:
        old_driver = db.query(Driver).filter(Driver.id == car.driver_id).first()
        if old_driver:
            old_driver.car_id = None

    # Привязываем
    car.driver_id = driver_uuid
    driver.car_id = car_id

    db.commit()

    return {
        "status": "OK",
        "message": "Водитель привязан к автомобилю успешно."
    }


# 8. Удалить водителей с рейтингом меньше указанного
@app.delete(
    "/api/drivers/cleanup",
    dependencies=[Depends(verify_token)],
    responses={401: {"model": ErrorResponse}}
)
def cleanup_drivers(
        maxRating: float,
        db: Session = Depends(get_db)
):
    # Находим водителей для удаления
    drivers_to_delete = db.query(Driver).filter(Driver.rating < maxRating).all()

    deleted_drivers = []
    for driver in drivers_to_delete:
        # Отвязываем от машины
        if driver.car_id:
            car = db.query(Car).filter(Car.id == driver.car_id).first()
            if car:
                car.driver_id = None

        deleted_drivers.append({
            "id": str(driver.id),  # UUID в строку
            "name": driver.name,
            "phone": driver.phone,
            "rating": driver.rating
        })

        # Удаляем водителя
        db.delete(driver)

    db.commit()

    return {
        "message": f"Удаление водителей с рейтингом ниже {maxRating} выполнено успешно",
        "deletedCount": len(deleted_drivers),
        "deletedDrivers": deleted_drivers
    }


# 9. Заменить водителя у машины
@app.put(
    "/api/cars/{car_id}/driver",
    dependencies=[Depends(verify_token)],
    responses={401: {"model": ErrorResponse}}
)
def replace_car_driver(
        car_id: int,
        driver_id: str,
        reason: Optional[str] = None,
        db: Session = Depends(get_db)
):
    # Парсим UUID нового водителя
    new_driver_uuid = parse_uuid(driver_id)

    # Получаем машину
    car = db.query(Car).filter(Car.id == car_id).first()
    if not car:
        raise HTTPException(status_code=404, detail="Машина не найдена")

    # Получаем предыдущего водителя
    previous_driver = None
    if car.driver_id:
        prev_driver = db.query(Driver).filter(Driver.id == car.driver_id).first()
        if prev_driver:
            previous_driver = {
                "id": str(prev_driver.id),
                "name": prev_driver.name
            }
            prev_driver.car_id = None

    # Получаем нового водителя
    new_driver = db.query(Driver).filter(Driver.id == new_driver_uuid).first()
    if not new_driver:
        raise HTTPException(status_code=404, detail="Новый водитель не найден")

    # Если у нового водителя уже есть машина - отвязываем
    if new_driver.car_id:
        old_car = db.query(Car).filter(Car.id == new_driver.car_id).first()
        if old_car:
            old_car.driver_id = None

    # Привязываем нового водителя
    car.driver_id = new_driver_uuid
    new_driver.car_id = car_id

    db.commit()

    return {
        "message": f"Водитель успешно заменен. {reason or ''}",
        "car": {
            "id": car.id,
            "number": car.number,
            "previousDriver": previous_driver,
            "newDriver": {
                "id": str(new_driver.id),
                "name": new_driver.name,
                "phone": new_driver.phone,
                "rating": new_driver.rating
            }
        }
    }


# 10. Получить водителей с рейтингом в диапазоне
@app.get(
    "/api/drivers/rating/range",
    dependencies=[Depends(verify_token)],
    responses={401: {"model": ErrorResponse}}
)
def get_drivers_by_rating_range(
        min: float,
        max: float,
        sort: Optional[str] = "desc",
        db: Session = Depends(get_db)
):
    if min > max:
        raise HTTPException(status_code=400, detail="min не может быть больше max")

    query = db.query(Driver).filter(Driver.rating.between(min, max))

    if sort == "desc":
        query = query.order_by(Driver.rating.desc())
    else:
        query = query.order_by(Driver.rating.asc())

    drivers = query.all()

    result = []
    for driver in drivers:
        driver_data = {
            "id": str(driver.id),  # UUID в строку
            "name": driver.name,
            "phone": driver.phone,
            "rating": driver.rating,
            "totalTrips": 0,
            "status": "active" if driver.car_id else "inactive",
            "currentCar": None
        }

        if driver.car_id:
            car = db.query(Car).filter(Car.id == driver.car_id).first()
            if car:
                driver_data["currentCar"] = {
                    "id": car.id,
                    "brand": car.brand
                }

        result.append(driver_data)

    return {
        "count": len(result),
        "ratingRange": f"{min}-{max}",
        "drivers": result
    }


# 11. Удалить машины по маркам (bulk-delete)
@app.delete(
    "/api/cars/bulk-delete",
    dependencies=[Depends(verify_token)],
    responses={401: {"model": ErrorResponse}}
)
def bulk_delete_cars(
        brands: List[str],
        reason: Optional[str] = None,
        db: Session = Depends(get_db)
):
    details = {}
    total_deleted = 0

    for brand in brands:
        # Находим машины этой марки
        cars_to_delete = db.query(Car).filter(Car.brand == brand).all()

        for car in cars_to_delete:
            # Отвязываем водителя
            if car.driver_id:
                driver = db.query(Driver).filter(Driver.id == car.driver_id).first()
                if driver:
                    driver.car_id = None

            # Удаляем машину
            db.delete(car)

        count = len(cars_to_delete)
        details[brand] = count
        total_deleted += count

    db.commit()

    return {
        "message": f"Машины указанных марок успешно удалены. {reason or 'Списание автопарка'}",
        "deletedCount": total_deleted,
        "deletedBrands": brands,
        "details": details
    }


# --- ЗАПУСК ---
if __name__ == "__main__":
    print("=" * 50)
    print("🚖 Taxi Fleet Admin API")
    print("=" * 50)
    print("✅ База данных: taxi_fleet.db")
    print("✅ Таблицы: car_statuses, drivers (с UUID и телефоном), cars")
    print("✅ Токен авторизации: secret_token_123")
    print("=" * 50)
    print("📚 Документация: http://localhost:8001/docs")
    print("=" * 50)

    uvicorn.run(app, host="127.0.0.1", port=8001)