from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RddOperations")
sc = SparkContext(conf = conf)

data1 = [1,2,2,3,3,3,5,6]
rdd1 = sc.parallelize(data1)

print(rdd1.count()) 
#Çıktı: 8

# lambda parametre1, parametre2,.. : yapılacak işlem
list1 = rdd1.map(lambda x: x+1).collect()
# map ile rdd üzerindeki tüm elemanları tek tek gezerek input olarak aldığımız fonksiyonu uygularız
# rdd bir action çağırmamızı beklediği için(lazy evaluation) collect() actionunu çağırıyoruz çıktı olarak bize liste dönüyor
print(list1) 
#Çıktı: [2, 3, 3, 4, 4, 4, 6, 7]

list2 = rdd1.filter(lambda x: x%2).collect()
# filter içine input olarak bir fonksiyon alır 1 dönenler filtre'den geçebiliyor fakat 0 dönenler geçemiyor
print(list2) 
#Çıktı: [1, 3, 3, 3, 5]

rdd = rdd1.flatMap(lambda x: (x,x*100,999))
# input olarak fonksiyon alır, her rdd elemanı için birden fazla(bu örnek için 3) çıktı almamızı sağlıyor
print(rdd.collect()) 
#Çıktı: [1, 100, 999, 2, 200, 999, 2, 200, 999, 3, 300, 999, 3, 300, 999, 3, 300, 999, 5, 500, 999, 6, 600, 999]
print(rdd.count()) 
#Çıktı: 24

list3 = rdd1.map(lambda x: (x,x*100,999)).collect()
print(list3) 
#Çıktı: [(1, 100, 999), (2, 200, 999), (2, 200, 999), (3, 300, 999), (3, 300, 999), (3, 300, 999), (5, 500, 999), (6, 600, 999)]

list4 = rdd1.distinct().collect()
# distinct rdd'de tekrar eden elemanlardan sadece 1 örnek bırakır.
print(list4)
#Çıktı: [1, 2, 3, 5, 6]

data2 = [100,101,2,3]
rdd2 = sc.parallelize(data2)

list5 = rdd1.union(rdd2).collect()
# union 2 rdd'yi birleştirir
print(list5)
#Çıktı: [1, 2, 2, 3, 3, 3, 5, 6, 100, 101, 2, 3]

list6 = rdd1.intersection(rdd2).collect()
# intersection 2 rdd'nin ortak elemanlarını(kesişim) dönüyor
print(list6)
#Çıktı: [2, 3]

list7 = rdd1.subtract(rdd2).collect()
# subtract rdd1'de olup rdd2'de olmayanelemanları verir. kümelerden bildiğimiz fark işlemi
print(list7)
#Çıktı: [6, 1, 5]

list8 = rdd2.subtract(rdd1).collect()
# rdd2'de olup rdd1'de olmayanlar
print(list8)
#Çıktı: [100, 101]

rdd3 = sc.parallelize([1,2,3,4,5])

# rdd'deki 2 elamanı alıp işlem yapıp yeni elemanı döndürür
# ardından toplanan sonuç x olur dizinin sonraki elemanı y olur
# döngü böyle dizi sonuna kadar gider
print(rdd3.reduce(lambda x,y : x+y))
#Çıktı: 15

# rdd'nin elemanını y olarak alır belirlenen zeroValue(3) ile inputta verdiğimiz fonksiyonu gerçekleştirir. 
# Bu fonksiyonun sonucu ikinci işlemin x'i olur ve yeni y değeri rdd'nin ikinci elemanı olur. zeroValue her adımda bulunur. (3*x*y)
# 3(zeroValue) x 1(rdd'nin ilk elemanı yani y) = 3(diğer işlemin x'i)
# 3(zeroValue) x 3(önceki işlemden gelen x) x 2(rdd'nin ikinci elemanı) = 18(diğer işlemin x'i)
# 3 x 18 x 3 = 162
# 3 x 162 x 4 = 1944
# 3 x 1944 x 5 = 29160
print(rdd3.fold(3,lambda x,y: x*y))
#Çıktı: 29160

print(rdd3.take(4))
# istediğimiz sayıda rdd'de bulunma sırasıyla elemanları döndürüyor
#Çıktı: [1, 2, 3, 4]

print(rdd3.top(3))
# büyükten küçüğe 3(kendi belirlediğimiz sayı kadar) tane eleman getirir
#Çıktı: [5, 4, 3]

print(rdd3.takeSample(True,5))
#Çıktı: [2, 2, 3, 5, 2]
# istediğimiz sayıda elemanları rdd'ye tekrar geri yerleştirerek ya da yerleştirmeden örnek elemanları yazdırıyor
# True olursa elemanları tekrar geri yerleştiriyor,yani kullandığımız rdd elemanını tekrar kullanabiliriz
# False yazarsak kullandığımız rdd elemanını bir daha kullanamayız.Her elemanı 1 defa kullanabiliriz

print(rdd3.countByValue())
# rdd'de her değerden kaç tane olduğunu sözlük tipinde döndürür
#Çıktı: {1: 1, 2: 1, 3: 1, 4: 1, 5: 1}
