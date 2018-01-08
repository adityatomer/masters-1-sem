#!/usr/bin/python
# -*- coding: UTF-8 -*-
import re
from pyspark import SparkContext
import os
from tifffile import TiffFile
import io
import zipfile
import numpy as np
from scipy import linalg
from skimage import data
import math
import hashlib

sc = SparkContext(appName="Aditya")

def getCompleteDataRDD(path):
    rdd = sc.binaryFiles(path)
    return rdd;

def getOrthoTif(zfBytes):
 #given a zipfile as bytes (i.e. from reading from a binary file),
 # return a np array of rgbx values for each pixel
 bytesio = io.BytesIO(zfBytes)
 zfiles = zipfile.ZipFile(bytesio, "r")
 #find tif:
 for fn in zfiles.namelist():
  if fn[-4:] == '.tif':#found it, turn into array:
   tif = TiffFile(io.BytesIO(zfiles.open(fn).read()))
   return tif.asarray()

#Convert a big matrix into smaller matrix, returns tuple of (Image, smaller matrix)
def getSmallerMat(fileName,fileMat,size,row,col):
    res = []
    st=0;
    en=size;
    k=0;
    for i in range(row):
        st_i=i*size
        en_i=(i+1)*size
        for j in range(col):
            st_j=j*size
            en_j=(j+1)*size
            temp=[]
            temp.append(fileName+"-"+str(k))
            temp.append(fileMat[st_i:en_i,st_j:en_j])
            res.append(tuple(temp))
            k+=1
    #print(fileNameToMatDict)
    return res


def print_1e_output(imgArrReducedMatRdd):
    output_1e_list = ['3677454_2025195.zip-0', '3677454_2025195.zip-1', '3677454_2025195.zip-18', '3677454_2025195.zip-19']
    output_1e_rdd=imgArrReducedMatRdd.filter(lambda x: x[0] in output_1e_list)
    output_1e_out_rdd = output_1e_rdd.map(lambda x : (x[0],x[1][0][0]))
    output_1e_out_rdd = output_1e_out_rdd.collect()
    print(output_1e_out_rdd)

#Convert a matrix into Intensity matrix, returns tuple of (image, intensity matrix)
def convertIntoFeatureVector(imgReducedMat):
    fileName=imgReducedMat[0]
    fileMat=imgReducedMat[1]
    newMatRow=len(fileMat)
    newMatCol=len(fileMat[0])
    newMat=np.zeros(shape=(newMatRow,newMatCol),dtype=int)
    #print("fileName: ",fileName)
    px_row=len(fileMat)
    px_col=len(fileMat[0])
    #print("px_row: ",px_row,", px_col: ",px_col)
    for px_i in range(px_row):
        for px_j in range(px_col):              
            pixelArr=fileMat[px_i][px_j]
            rgb_mean=(int(pixelArr[0])+int(pixelArr[1])+int(pixelArr[2]))/3
            infrared=pixelArr[3]
            intensity=int(rgb_mean * (infrared/float(100)))
            newMat[px_i][px_j]=int(intensity)
    return (fileName,newMat) 

#Convert a big matrix into smaller matrix with given row and column
def convertToSmallMat(intensityMat,size,row,col):
    fileName=intensityMat[0]
    fileMat=intensityMat[1]
    #print(fileName)
    res = [[0 for q in range(col)] for w in range(row)]
    st=0;
    en=size;
    for p in range(row):
        st_i=p*size
        en_i=(p+1)*size
        for q in range(col):
            st_j=q*size
            en_j=(q+1)*size
            #npFileMat=np.array(fileMat)
            res[p][q]=(fileMat[st_i:en_i,st_j:en_j]).sum()
    return (fileName,res) 

# Calculate row difference in intensities. Returns tuple of (Image, matrix)
def rowDifferenceInIntensities(reducedResolutionImageRdd):
    fileName=reducedResolutionImageRdd[0]
    fileMat=reducedResolutionImageRdd[1]
    fileMat=np.diff(fileMat)
    fileMat[fileMat > 1]=1
    fileMat[fileMat < -1]=-1
    return (fileName,fileMat)

# Calculate column difference in intensities. Returns tuple of (Image, matrix)
def colDifferenceInIntensities(reducedResolutionImageRdd):
    fileName=reducedResolutionImageRdd[0]
    fileMat=reducedResolutionImageRdd[1]
    fileMat=np.diff(fileMat,axis=0)
    fileMat[fileMat > 1]=1
    fileMat[fileMat < -1]=-1
    return (fileName,fileMat)

# Convert a matrix into list of list.
def flatten(rddd):
    fileName=rddd[0]
    fileMat=rddd[1]
    fileMat=fileMat.flatten()
    return (fileName,fileMat)

# Concatenate two arrays
def concatenateMatrix(x1,x2):
    return np.concatenate((x1, x2), axis=0)

#Perfectly breaks image feature into 128 chunks of 38 or 39 characters.
def get2sizes_of_elements_38_and_39(rg):
    k1=0;k2=0
    for i in range(math.floor(rg/38)):
        k1=(rg-(i*39))/float(38)
        if(k1==math.floor(k1)):
            k2=i
            k1=math.floor(k1)
            break;
    return (k1,k2)

#Given a feature matrix returns a hash for a image
def chunkifyInto128Bits(features):
    #92*38+36*39
    two_size = get2sizes_of_elements_38_and_39(4900) #first element is of size 38 and second is of size 39 elements.
    array_size_1 = 38*two_size[0]
    array_size_2 = 39*two_size[1]
    array_1 = features[0:array_size_1]
    array_2= features[array_size_1:(array_size_1+array_size_2)]
    array_92size_38elements=np.split(array_1,array_size_1/38)
    array_36size_39elements=np.split(array_2,array_size_2/39)
    imageHash=''
    for x in array_92size_38elements:        
        hshX = hashlib.md5(x).hexdigest()
        imageHash+=hshX[0]
    for x in array_36size_39elements:
        hshX = hashlib.md5(x).hexdigest()
        imageHash+=hshX[0]
    return imageHash  


#given a band size, Image signature and Mod, returns array of [(band index, mod value) , ()]
def imageToHash(band,imgSig,mod):
    #bands = [4]#[1, 2, 4, 8, 16, 32,64]
    bandCharArray=[]
    # for band in bands:
    st=0
    en=st+band
    bandIndex=0
    while(st<128):
        bandStr=imgSig[st:en]
        hashVal = hashlib.sha1(bandStr.encode('utf-8')).hexdigest()
        val=int(hashVal, 16) % (mod)
        temp = []
        temp.append(bandIndex)
        temp.append(val)
        bandCharArray.append(tuple(temp))
        bandIndex+=1
        st=en
        en=en+band            
    return bandCharArray


# Convert to tuple of ((band,bucket), Image)
def convertToBandBucketToImg(imageToHash):
    imgName=imageToHash[0]
    imgBandList=imageToHash[1]
    modArr = []
    for tup in imgBandList:
        temp=[]
        temp.append(tup)
        temp.append(imgName)
        modArr.append(tuple(temp))
    return modArr 

#returns a tuple of two values (imgToFind , array of similar images)
def findCommonImages(imgToFind,ll):
    imageSet=set()
    for l in ll:
        for img in l:
            if(img !=imgToFind):
                imageSet.add(img)
    return (imgToFind,list(imageSet))

def findSVD(featureMat):
    mean, stdDev =np.mean(featureMat, axis=0), np.std(featureMat, axis=0)
    stdDev[stdDev==0]=1
    fm_zs = (featureMat - mean) / stdDev  
    U, s, Vh = linalg.svd( fm_zs, full_matrices=0, compute_uv=1 )
    low_dim_p = 10
    Vtrans = Vh.transpose()
    return Vtrans[:,0:10]

#To Multiply two matrices
def multiplyMat(imgMat,svdMatrix):
    temp=[]
    temp.append(imgMat)
    return np.matmul(temp,svdMatrix)

#To Find Eucledean Distance between a two matrices. 
# Key=given Image(mat), 
# Value = Array of Image simlar(similarMatArry) to given matrix(mat)
# reducedImagesMap = Map of similar Image to Matrix
def findEucledeanDistance(mat,similarMatArry,reducedImagesMap):
    dist = []
    for similarMat in similarMatArry:
        temp=[]
        temp.append(similarMat)
        temp.append(np.sqrt(np.sum((reducedImagesMap[mat] - reducedImagesMap[similarMat]) ** 2)))        
        dist.append(tuple(temp))
    dist=sorted(dist,key=lambda x: x[1])
    return dist

def main():
    rdd=None
    # path='/Users/adityatomer/Desktop/bigdata_a2/data/small_sample/'
    path='hdfs:/data/large_sample/'
    rdd=getCompleteDataRDD(path);
    fileNamesRdd=rdd.map(lambda x: x[0]).map(lambda x: x.split("/")[-1])
    #########################
    #1a.
    print("\n**********************>>>> FileNames <<<<<<************************")
    print(fileNamesRdd.collect())
    imgArrRdd=rdd.map(lambda x: (x[0].split("/")[-1],getOrthoTif(x[1])))
    imgArrReducedMatRdd=imgArrRdd.flatMap(lambda x: getSmallerMat(x[0],x[1],500,5,5))
    #########################
    #1e.
    print("\n**********************>>>> 1.e <<<<<<************************")
    print_1e_output(imgArrReducedMatRdd)
    
    ##################################################
    #2nd & 3rd
    intensityArrMatRdd=imgArrReducedMatRdd.map(lambda x : convertIntoFeatureVector(x))
    intensityArrMatRdd.persist()
    # Also taking care of 3D here. Making a list of configurable parameters here
    # Will run twice for resolution factors 10 and 5(3.D Extra Credit)
    resolution_reduction_factors=[10,5]
    bands = [16,8]
    bucketsMod = [500,1000]

    for i in range(len(resolution_reduction_factors)):
        print("\n\n\n****************************************************************************************")
        print("Reducing image resolution by a factor of : ",resolution_reduction_factors[i],", band selection: ",bands[i],", buckets: ",bucketsMod[i])
        print("****************************************************************************************")
        px_row=px_col=int(500/resolution_reduction_factors[i])
        ##################################################
        #2B
        reducedResolutionImagesRdd=intensityArrMatRdd.map(lambda x : convertToSmallMat(x,resolution_reduction_factors[i],px_row,px_col))

        ##################################################
        #2C
        row_dif=reducedResolutionImagesRdd.map(lambda x : rowDifferenceInIntensities(x))

        ##################################################
        #2D
        col_dif=reducedResolutionImagesRdd.map(lambda x : colDifferenceInIntensities(x))

        ##################################################
        #2D
        flatten_row=row_dif.map(lambda x: flatten(x))
        flatten_col=col_dif.map(lambda x: flatten(x))
        
        ##################################################
        #2E
        features = flatten_row.join(flatten_col)
        features_concat = features.map(lambda x : (x[0],concatenateMatrix(x[1][0],x[1][1])))

        ##################################################
        #2F
        imgList = ['3677454_2025195.zip-1','3677454_2025195.zip-18']
        final_2f_img_rdd=features_concat.filter(lambda x : x[0] in imgList)
        final_2f_img_rdd_collect = final_2f_img_rdd.collect()
        print("\n**********************>>>> <2nd F> <<<<<<************************")
        print(final_2f_img_rdd_collect)
        

        ##################################################
        #3a
        imageToSignatureRdd = features_concat.map(lambda x: (x[0],chunkifyInto128Bits(x[1])))

        #for j in range(len(bands)):
        print("\n\n\n********************************************************")        
        ##################################################
        #3b.i
        imgToBandBucketListRdd = imageToSignatureRdd.map(lambda x : (x[0],imageToHash(bands[i],x[1],bucketsMod[i])))

        ##################################################
        #3b.ii
        bandBucketToImgRdd = imgToBandBucketListRdd.flatMap(lambda x : convertToBandBucketToImg(x))
        bandBucketToImgListRdd = bandBucketToImgRdd.groupByKey()
        bucketBandToCommonImgsListRdd = bandBucketToImgRdd.join(bandBucketToImgListRdd)
        ImgToCommonImagesRdd = bucketBandToCommonImgsListRdd.map(lambda x : (x[1][0],x[1][1])).mapValues(list)
        ImgToCommonImagesGroupedRdd = ImgToCommonImagesRdd.groupByKey().mapValues(list)
        finalImgToCommonImgRdd = ImgToCommonImagesGroupedRdd.map(lambda x : findCommonImages(x[0],x[1]))

        ##################################################
        #3.b.iii
        #print the 20 candidates for 3677454_2025195.zip-1, 3677454_2025195.zip-18
        imgList = ['3677454_2025195.zip-0', '3677454_2025195.zip-1', '3677454_2025195.zip-18', '3677454_2025195.zip-19']
        imgListToPrint = ['3677454_2025195.zip-1', '3677454_2025195.zip-18']
        finalFilteredImgListRdd = finalImgToCommonImgRdd.filter(lambda x :  x[0] in imgList)
        finalFilteredImgListToPrint = finalFilteredImgListRdd.filter(lambda x : x[0] in imgListToPrint).collect()
        print("\n**********************>>>> <3.B Candidate Arrays> <<<<<<************************")
        print(finalFilteredImgListToPrint)
        ##################################################
        #3.c.i
        fetMat = features_concat.map(lambda x: x[1]).takeSample(False,10)
        svdMatrix=findSVD(fetMat)
        reducedImagesRdd=features_concat.map(lambda x : (x[0],multiplyMat(x[1],svdMatrix)))
        reducedImagesMap = reducedImagesRdd.collectAsMap()

        ##################################################
        #3.c.ii
        finalFilteredImgEDList = finalFilteredImgListRdd.map(lambda x : (x[0],findEucledeanDistance(x[0],x[1],reducedImagesMap)))
        ##################################################
        #3.c.iii
        finalFilteredImgEDList_collect = finalFilteredImgEDList.collect()
        print("\n**********************>>>> <3.C Candidate Eucledean Distance> <<<<<<************************")
        print(finalFilteredImgEDList_collect)


if __name__ == '__main__':
    main()
