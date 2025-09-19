
CREATE TABLE hotels (hotel_id STRING PRIMARY KEY, hotel_name STRING, stars INT, country STRING);
CREATE TABLE rooms (room_id STRING PRIMARY KEY, hotel_id STRING REFERENCES hotels(hotel_id), room_type_code STRING, room_type_desc STRING, max_occupancy INT);
CREATE TABLE customers (customer_id STRING PRIMARY KEY, first_name STRING, last_name STRING, email STRING, country STRING, gdpr_optin TINYINT);
CREATE TABLE bookings (booking_id STRING PRIMARY KEY, customer_id STRING REFERENCES customers(customer_id), hotel_id STRING REFERENCES hotels(hotel_id), room_id STRING REFERENCES rooms(room_id), created_at DATE, checkin_date DATE, checkout_date DATE, nights INT, currency STRING, total_amount DECIMAL(12,2), status STRING, source STRING);
CREATE TABLE payments (payment_id STRING PRIMARY KEY, booking_id STRING REFERENCES bookings(booking_id), provider STRING, status STRING, amount DECIMAL(12,2), currency STRING, transaction_date DATE);
