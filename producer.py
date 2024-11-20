import sys
# Compatibilidade para versões do Python >= 3.12
if sys.version_info >= (3, 12):
    import six.moves # type: ignore
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
from pprint import pprint

# Inicialização do gerador de dados fictícios e do produtor Kafka
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Lista de 20 produtos predefinidos
PRODUCTS = [
    "Laptop", "Smartphone", "Tablet", "Monitor", "Keyboard",
    "Mouse", "Headphones", "Smartwatch", "Camera", "Printer",
    "Router", "SSD", "Hard Drive", "RAM", "Graphics Card",
    "Processor", "Motherboard", "Power Supply", "Cooling Fan", "Webcam"
]

def generate_order():
    return {
        "order_id": fake.uuid4(),
        "client_document": fake.ssn(),
        "products": [
            {
                "product_name": random.choice(PRODUCTS),
                "quantity": random.randint(1, 5),
                "price": round(random.uniform(10, 100), 2)
            } for _ in range(random.randint(1, 3))
        ],
        "total_value": 0,
        "sale_datetime": fake.iso8601()
    }

def calculate_total(order):
    return sum(product["quantity"] * product["price"] for product in order["products"])

def send_order(producer, topic):
    # Formata e envia uma ordem ao tópico Kafka
    order = generate_order()
    order['total_value'] = calculate_total(order)
    producer.send(topic, value=order)
    print("Sent order:")
    pprint(order)  # Usando pprint para formatar a saída

def main():
    topic = 'ecommerce_sales'
    try:
        while True:
            send_order(producer, topic)
            time.sleep(3)  # Envia uma mensagem a cada 3 segundos
    except KeyboardInterrupt:
        print("Processo interrompido pelo usuário.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
