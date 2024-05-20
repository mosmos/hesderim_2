from osgeo import  gdal
from osgeo import osr
import argparse
import glob
import os
import time
import subprocess #v1.2

version = 1.2


start_time = time.time()

# globals
IMG_PIXELS = 512
PIXEL_WIDTH = 20.0/512

def calc_pixel_size(tiles_folder,image_format='tif'):
        
        files_to_mosaic = glob.glob(tiles_folder + "\*."+image_format)
        if len(files_to_mosaic)==1:
            return PIXEL_WIDTH
        X_list=[]
        Y_list=[]
        for img in files_to_mosaic:
            img_name = os.path.basename(img).split('.')[0]
            minX = int(img_name.split('_')[0]) 
            maxY = int(img_name.split('_')[1])  
            X_list.append(minX)
            Y_list.append(maxY)
        X_set= set(X_list)
        Y_set= set(Y_list)

        if len(X_set)>len(Y_set):
            img_pixelWidth = (max(X_set)-min(X_set))/(len(X_set)-1)
        else:
            img_pixelWidth = (max(Y_set)-min(Y_set))/(len(Y_set)-1)

        return img_pixelWidth/IMG_PIXELS   


def set_geotransform_by_XY(img, startX, startY, img_pixel_size ):

    if not img:
        return None
    
    try:
        tile_img = gdal.Open(img,1)   # open the new image for editing!
        tile_img.SetGeoTransform((startX,img_pixel_size,0,startY,0,-img_pixel_size)) #set geotif tags to the new image 
        # set projection 
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(2039)
        tile_img.SetProjection(srs.ExportToWkt())            
        tile_img = None
    except:
        print ('[!err!] setting geotrans...')
        img = None
    return img        
 
 
def georef_stamps(stamp_folder,image_format='tif'):
    #print ('[info] setting geotransform')
    files_to_mosaic = glob.glob(stamp_folder + "\*."+image_format)
    print (f"found: {len(files_to_mosaic)} tiles")
    img_pixelWidth = calc_pixel_size(stamp_folder,image_format='tif')
    #img_pixelWidth =  PIXEL_WIDTH

    for img in files_to_mosaic:
        img_name = os.path.basename(img).split('.')[0]
        minX = int(img_name.split('_')[0]) 
        maxY = int(img_name.split('_')[1])  
        set_geotransform_by_XY(img, minX, maxY, img_pixelWidth)
    print ("tile sample name:",img_name)
    print ("tile minX:",minX)
    print ("tile maxY:",maxY)    


def main ():

    parser = argparse.ArgumentParser(description = "Creates a single compressed GeoTIFF from a folder of smaller GeoTIFFs")
    parser.add_argument("-o", "--output", help = "Output file name from SQL. eg: 20240220141725787_756D2")
    parser.add_argument("-i", "--input", help = "Input tiff folder from SQL. eg: \\nas01\TeumHandasyFile\Dev\HesdereyTnua\RasterFiles\202402201416_38098\756D2")
    args = parser.parse_args()
    #print (args.__dict__ )

    if not args.output:
        img_name = r"\\gisppr01\d$\GeoServer\data_dir\coverages\hesderim\20240306103551854_759A9" 
    else:
        img_name = args.output

    if not args.input:
        img_folder = r"\\nas01\TeumHandasyFile\Preprod\HesdereyTnua\RasterFiles\20240306103507367_38585_38585\759A9"
    else:
        img_folder = args.input
    
    input_files=glob.glob(img_folder + "\*.tif")    
    output_file = img_name  + ".tif"

    print ("param -i:",args.input)
    print ("param -o:",args.output)
    
    print ('[info] setting geotransform')
    georef_stamps(img_folder,image_format='tif')
    print ("[info] creating img...",output_file)
    
 
# v1.2
    cmd = f"gdalwarp {img_folder}/*.tif {output_file} -co compress=lzw"
    try:
        subprocess.run(cmd, check=True)
    except Exception as e:
        print ("ERR gdalwarp:",e)
#
 
    end_time = time.time()
    execution_time = end_time - start_time

    print ("------------------------------------")
    print ("total time:",execution_time,"s")
    print ("total tiles:",len(input_files))
    print ("output",output_file)
    print ("....................................")


if __name__ == '__main__':
    main()