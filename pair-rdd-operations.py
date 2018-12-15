from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PairRddOperations")
sc = SparkContext(conf = conf)

data1 =[('Fzk-I',73), ('Mat-I',64),('Kimya',66),('Mat-I',44),('Mat-I',78)]
rdd1 = sc.parallelize(data1)

list1 = rdd1.reduceByKey(lambda x,y: x+y).collect()
# reduce actionuyla aynı çalışma mantığına ek olarak aynı anahtar değerine sahip elemanlar üzerinde işlem yapıyor 
print(list1)
#Çıktı: [('Fzk-I', 73), ('Mat-I', 186), ('Kimya', 66)]

list2 = rdd1.groupByKey().collect()
# anahtarları aynı olan elemanları bir liste halinde yazıyor
for i in list2:
    print(i[0],list(i[1]))
#Çıktı: Fzk-I [73]
#       Mat-I [64, 44, 78]
#       Kimya [66]

list3 = rdd1.groupByKey().map(lambda t: (t[0],sum(t[1]))).collect()
print(list3)
# yukarıdaki reduceByKey örneği ile aynı çıktıyı almak için groupByKey'i de kullanabiliriz
#Çıktı: [('Fzk-I', 73), ('Mat-I', 186), ('Kimya', 66)]  

data2 = [('Mat-I',68),('Fzk-I',45),('Kimya',94),('Tarih',79)]
rdd2 = sc.parallelize(data1+data2)
# data1 ve data2'yi toplayıp rdd2'de birleştirdik

toplam = rdd2.combineByKey((lambda x: (x,1)), # createCombiner: ilk kez bir anahtar bulunduğunda çağrılır. Bu fonk, bu anahtar için bir toplayıcı oluşturur
    (lambda x, y: (x[0] + y, x[1] + 1)), # mergeValue: Anahtar zaten bir toplayıcıya sahip ise mergeValue çağrılır
    (lambda x, y: (x[0] + y[0], x[1] + y[1])) ) # mergeCombiner: iki fonksiyonun sonuçlarını birleştirir
print(toplam.collect())
#Çıktı: [('Fzk-I', (118, 2)), ('Mat-I', (254, 4)), ('Kimya', (160, 2)), ('Tarih', (79, 1))]

print(toplam.map(lambda x : (x[0], x[1][0] / x[1][1])).collect()) 
# her dersten alınan ortalama notları hesaplıyoruz
#Çıktı: [('Fzk-I', 59.0), ('Mat-I', 63.5), ('Kimya', 80.0), ('Tarih', 79.0)]

list4 = rdd2.mapValues(lambda x: x+1).collect()
# anahtar değeri değiştirmeden sadece değerler üzerinde işlem yapılacağı zaman kullanılır
print(list4)
#Çıktı: [('Fzk-I', 74), ('Mat-I', 65), ('Kimya', 67), ('Mat-I', 45), ('Mat-I', 79), ('Mat-I', 69), ('Fzk-I', 46), ('Kimya', 95), ('Tarih', 80)]

list5 = rdd2.map(lambda x: (x[0], x[1]+1)).collect() 
print(list5)
#Çıktı:[('Fzk-I', 74), ('Mat-I', 65), ('Kimya', 67), ('Mat-I', 45), ('Mat-I', 79), ('Mat-I', 69), ('Fzk-I', 46), ('Kimya', 95), ('Tarih', 80)]
# Çıktısı yukarıdaki ile aynı. sadece değerleri değişeceksek mapValues kullanmak doğru

rdd3 = sc.parallelize([('meyve','elma,muz,portakal'),('hayvan','zero,alisthu'),('esya','lamba,masa,kitap')])

list6 = rdd3.flatMapValues(lambda x: x.split(',')).collect()
# anahtar değerler ile yeni ikili değerler oluşturuluyor
print(list6)
#Çıktı: [('meyve', 'elma'), ('meyve', 'muz'), ('meyve', 'portakal'), ('hayvan', 'zero'), ('hayvan', 'alisthu'), ('esya', 'lamba'), ('esya', 'masa'), ('esya', 'kitap')]

print(rdd3.keys().collect())
# sadece anahtarları içeren bir rdd döndürür
#Çıktı: ['meyve', 'hayvan', 'esya']

print(rdd3.values().collect())
# sadece değerleri içeren bir rdd döndürür
#Çıktı: ['elma,muz,portakal', 'zero,alisthu', 'lamba,masa,kitap']

print(rdd3.sortByKey().collect())
#Çıktı: [('esya', 'lamba,masa,kitap'), ('hayvan', 'zero,alisthu'), ('meyve', 'elma,muz,portakal')]
# anahtarları küçükten büyüğe (alfabetik) olarak sıralıyor

rdd4 = sc.parallelize([('hayvan','panda,zebra'),('dil','python,java,scala')])

list7 = rdd3.subtractByKey(rdd4).collect()
# rdd3'ten rdd4'te de aynı anahtara sahip elemanları çıkarır
print(list7)
#Çıktı: [('meyve', 'elma,muz,portakal'), ('esya', 'lamba,masa,kitap')]

list8 = rdd3.join(rdd4).collect()
# iki rdd'yi ortak anahtarlara göre birleştirir. bu örnekte tek ortak anahtar 'hayvan'
print(list8)
#Çıktı: [('hayvan', ('zero,alisthu', 'panda,zebra'))]

list9 = rdd3.rightOuterJoin(rdd4).collect()
# rdd4'te bulunan değerleri rdd3 ile anahtarlara göre birleştirir
print(list9)
#Çıktı: [('hayvan', ('zero,alisthu', 'panda,zebra')), ('dil', (None, 'python,java,scala'))]

list10 = rdd3.leftOuterJoin(rdd4).collect()
# rdd3'de bulunan değerleri rdd4 ile anahtarlara göre birleştirir
print(list10)
#Çıktı: [('meyve', ('elma,muz,portakal', None)), ('hayvan', ('zero,alisthu', 'panda,zebra')), ('esya', ('lamba,masa,kitap', None))]


t=tuple()
for i in rdd3.cogroup(rdd4).collect():
    l=list()
    for a in range(len(list(i[1]))):
        l.append(list(i[1][a]))
    t+=(i[0],l)
print(t)  
# rdd'leri anahtarlarına göre gruplayıp birleştirir
#Çıktı: ('meyve', [['elma,muz,portakal'], []], 'hayvan', [['zero,alisthu'], ['panda,zebra']], 
# 'esya', [['lamba,masa,kitap'], []], 'dil', [[], ['python,java,scala']])

rdd5 = sc.parallelize([(1,'a,b'),(2,'b,c'),(3,'b'),(1,'d'),(1,'a,b'),(5,'a,b'),(6,'c'),(4,'a'),(3,'e')]) 

print(rdd5.countByKey())
# anahtarları sayar {anahtar:anahtar sayısı} şeklinde sözlük tipinde çıktı verir
#Çıktı: {1: 3, 2: 1, 3: 2, 5: 1, 6: 1, 4: 1})

print(rdd5.countByValue())
# hem anahtarı hem değeri aynı olan yani birebir aynı olan elemanları sayıyor sözlük tipinde çıktı verir
#Çıktı: {(1, 'a,b'): 2, (2, 'b,c'): 1, (3, 'b'): 1, (1, 'd'): 1, (5, 'a,b'): 1, (6, 'c'): 1, (4, 'a'): 1, (3, 'e'): 1}

d=[(1,2),(1,3),(3,2),(2,3),(3,4)]

print(sc.parallelize(d).reduceByKey(lambda x,y: x+y).getNumPartitions())
#spark'ın rdd üzerinde reduceByKey işlemi için kaç partition kullandığını verir
#Çıktı: 1
