import os
import requests
import boto3
import json
import folder_paths
from PIL import Image
from PIL.PngImagePlugin import PngInfo
import numpy as np

class S3Store:
    """
    A example node

    Class methods
    -------------
    INPUT_TYPES (dict): 
        Tell the main program input parameters of nodes.

    Attributes
    ----------
    RETURN_TYPES (`tuple`): 
        The type of each element in the output tulple.
    RETURN_NAMES (`tuple`):
        Optional: The name of each output in the output tulple.
    FUNCTION (`str`):
        The name of the entry-point method. For example, if `FUNCTION = "execute"` then it will run Example().execute()
    OUTPUT_NODE ([`bool`]):
        If this node is an output node that outputs a result/image from the graph. The SaveImage node is an example.
        The backend iterates on these output nodes and tries to execute all their parents if their parent graph is properly connected.
        Assumed to be False if not present.
    CATEGORY (`str`):
        The category the node should appear in the UI.
    execute(s) -> tuple || None:
        The entry point method. The name of this method must be the same as the value of property `FUNCTION`.
        For example, if `FUNCTION = "execute"` then this method's name must be `execute`, if `FUNCTION = "foo"` then it must be `foo`.
    """
    def __init__(self):
        self.output_dir = folder_paths.get_output_directory()
        self.type = "output"
    
    @classmethod
    def INPUT_TYPES(s):
        return {"required": 
                    {"images": ("IMAGE", ),
                     "filename_prefix": ("STRING", {"default": "ComfyUI"})},
                "hidden": {"prompt": "PROMPT", "extra_pnginfo": "EXTRA_PNGINFO"},
                }

    RETURN_TYPES = ()

    FUNCTION = "send_to_s3"

    OUTPUT_NODE = True

    CATEGORY = "S3"

    def send_to_s3(self, images, filename_prefix="ComfyUI", prompt=None, extra_pnginfo=None):
        
        # Set up AWS credentials
        s3_client = boto3.client('s3',
                                region_name=os.environ['S3_REGION'],
                                aws_access_key_id=os.environ['S3_API_KEY'],
                                aws_secret_access_key=os.environ['S3_SECRET_KEY'])

        def compute_vars(input):
            input = input.replace("%width%", str(images[0].shape[1]))
            input = input.replace("%height%", str(images[0].shape[0]))
            return input

        filename_prefix = compute_vars(filename_prefix)

        subfolder = os.path.dirname(os.path.normpath(filename_prefix))
        filename = os.path.basename(os.path.normpath(filename_prefix))

        full_output_folder = os.path.join(self.output_dir, subfolder)

        if os.path.commonpath((self.output_dir, os.path.abspath(full_output_folder))) != self.output_dir:
            print("Saving image outside the output folder is not allowed.")
            return {}

        results = list()
        for image in images:
            i = 255. * image.cpu().numpy()
            img = Image.fromarray(np.clip(i, 0, 255).astype(np.uint8))
            metadata = PngInfo()
            if prompt is not None:
                metadata.add_text("prompt", json.dumps(prompt))
            if extra_pnginfo is not None:
                for x in extra_pnginfo:
                    metadata.add_text(x, json.dumps(extra_pnginfo[x]))

            file = f"{filename}.png"
            current_output_image_path = os.path.join(full_output_folder, file)
            img.save(current_output_image_path, pnginfo=metadata, compress_level=4)

            results.append({
                "filename": file,
                "subfolder": subfolder,
                "type": self.type
            })

            s3_bucket_name = os.environ["S3_BUCKET_NAME"]
            s3_folder_name = os.environ["S3_IMAGE_FOLDER"]
            
            with open(current_output_image_path, 'rb') as f:
                s3_client.upload_fileobj(f, s3_bucket_name, f'{s3_folder_name}/{filename}.png')

            # Send a POST request to the Go server with the prediction ID and S3 URL
            s3_url = os.environ['S3_IMAGE_URL'] + filename + '.png'
            data = {'prediction_id': filename, 's3_url': s3_url}
            post_url = os.environ['POST_URL']
            requests.post(post_url, data=data)

        return { "ui": { "images": results } }


# A dictionary that contains all nodes you want to export with their names
# NOTE: names should be globally unique
NODE_CLASS_MAPPINGS = {
    "S3Store": S3Store
}

# A dictionary that contains the friendly/humanly readable titles for the nodes
NODE_DISPLAY_NAME_MAPPINGS = {
    "S3Store": "SendToS3"
}
