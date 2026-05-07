import http from 'k6/http';
import { check } from 'k6';

// Конфиг нагрузки
export const options = {
    scenarios: {
        steady_load: {
            executor: 'constant-arrival-rate',
            rate: 125, // среднее между 100 и 150 RPS
            timeUnit: '1s',
            duration: '300s',
            preAllocatedVUs: 200, // запас VU
            maxVUs: 400,
        },
    },
};

// Данные
const products = ['Tablet', 'Desktop', 'Smartphone'];

function randomEmail() {
    // ~20% запросов с фиксированным email
    if (Math.random() < 0.2) {
        return 'krevskiigrigorii@gmail.com';
    }
    return `user_${Math.random().toString(36).substring(2, 10)}@test.com`;
}

function randomProduct() {
    return products[Math.floor(Math.random() * products.length)];
}

export default function () {
    const url = 'http://localhost:8081/api/order';

    const payload = JSON.stringify({
        email: randomEmail(),
        productName: randomProduct(),
        quantity: 1,
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(url, payload, params);

    check(res, {
        'status is 2xx': (r) => r.status >= 200 && r.status < 300,
    });
}