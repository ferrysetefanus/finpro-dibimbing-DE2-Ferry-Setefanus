# finpro-dibimbing-DE2-Ferry-Setefanus

## Background
Tema dari final project saya adalah melakukan analisis terhadap data kejahatan yang terjadi di kota los angeles dengan tujuan untuk menemukan pola - pola kejahatan seperti apa yang sering terjadi di los angeles dari tahun 2020 hingga 2023 sehingga dapat memunculkan insight mengenai kejahatan apa saja yang sering terjadi, di mana kejahatan tersebut terjadi, kapan saja terjadinya, dll. Sehingga expected output dari final project ini adalah menghasilkan sebuah dashboard yang menampilkan informasi - informasi tersebut melalui sebuah pipeline yang menggunakan arsitektur batch processing untuk memproses data secara terjadwal.

## Arsitektur yang digunakan
![arsitektur](https://github.com/ferrysetefanus/finpro-dibimbing-DE2-Ferry-Setefanus/blob/main/img/architecture.png)

Extract file dari api -> transform menggunakan spark -> load ke hydra -> visualisasi data menggunakan metabase

## Result
![arsitektur](https://github.com/ferrysetefanus/finpro-dibimbing-DE2-Ferry-Setefanus/blob/main/img/metabase-result.png)

1. 2022 Merupakan tahun dengan tingkat kejahatan tertinggi
2. Central Los Angeles menjadi wilayah yang paling banyak terjadi kasus kriminal.
3. Gender tidak berpengaruh terhadap terjadinya kasus kriminal.
4. Jalanan menjadi tempat yang paling banyak terjadi kasus kriminal.
5. Kendaran yang dicuri menjadi kasus kriminal yang paling banyak terjadi di jalanan.
6. Jam 12 Siang menjadi waktu yang paling sering terjadi kasus kriminal.


