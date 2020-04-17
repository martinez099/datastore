import json

from redis import StrictRedis
from flask import Flask
from flask import request

from datastore import DataStore


app = Flask(__name__)
ds = DataStore(StrictRedis(decode_responses=True, host="localhost", port=6379))


@app.route('/product/<product_id>', methods=['GET'])
def get_product(product_id):

    # get product
    values = ds.get_product(product_id)

    return json.dumps(values)


@app.route('/product', methods=['POST'])
def post_product():

    # parse request body
    values = json.loads(request.data)

    # generate product id
    product_id = ds.new_product_id()

    # set category
    category_id = ds.get_category_id(values['MainCategoryName'])
    if not category_id:
        category_id = ds.new_category_id()
        ds.set_category(category_id, values['MainCategoryName'])
    ds.add_product_to_category(product_id, category_id)
    ds.set_category_name_idx(values['MainCategoryName'], category_id)
    ds.incr_category_rnk(values['MainCategoryName'])

    # set images
    img_ids = []
    for img in values['Images']:
        img_id = ds.new_image_id()
        img_ids.append(img_id)
        ds.set_image(img_id, img)
    ds.add_product_image_ids(img_ids, product_id)
    values['Images'] = 'product:{}:images'.format(product_id)

    # set product
    ds.set_product_name_idx(values['Name'], product_id)
    ds.set_product(product_id, values)

    return json.dumps(product_id)


@app.route('/product/<product_id>', methods=['PUT'])
def put_product(product_id):

    # product to update to
    new_values = json.loads(request.data)

    # product to be updated
    old_values = ds.get_product(product_id)

    # check category
    if new_values['MainCategoryName'] != old_values['MainCategoryName']:

        old_category_id = ds.get_category_id(old_values['MainCategoryName'])

        # remove old category
        category_id = old_category_id
        category = ds.get_category(category_id)
        ds.decr_category_rnk(category['Name'])
        ds.rem_category_name_idx(category['Name'])
        ds.rem_product_from_category(product_id, category_id)

        # add new category
        category_id = ds.get_category_id(new_values['MainCategory'])
        if not category_id:
            category_id = ds.new_category_id()
            ds.set_category(category_id, new_values['MainCategoryName'])
        ds.set_category_name_idx(new_values['MainCategoryName'], category_id)
        ds.incr_category_rnk(new_values['MainCategoryName'])
        ds.add_product_to_category(product_id, category_id)
        new_values['MainCategory'] = category_id

    # check images
    if set(new_values['Images']) != set(old_values['Images']):

        # remove old images
        imgs_to_remove = set(old_values['Images']) - set(new_values['Images'])
        for img in imgs_to_remove:
            img_id = ds.get_image_id(img)
            ds.rem_image(img_id)
            ds.rem_images_from_product(product_id, img_id)

        # add new images
        new_imgs = set(new_values['Images']) - set(old_values['Images'])
        for img in new_imgs:
            img_id = ds.new_image_id()
            ds.set_image(img_id, img)
            ds.add_product_image_ids(product_id, img_id)
        new_values['Images'] = 'product:{}:images'.format(product_id)

    # set product
    ds.set_product_name_idx(new_values['Name'], product_id)
    updated = ds.set_product(product_id, new_values)

    return json.dumps(updated)


@app.route('/product/<product_id>', methods=['DELETE'])
def delete_prodcut(product_id):

    # product to be deleted
    values = ds.get_product(product_id)

    # product
    deleted = ds.rem_product(product_id)
    category_id = ds.get_category_id(values['MainCategoryName'])
    ds.rem_product_from_category(product_id, category_id)
    ds.rem_product_name_idx(values['Name'])

    # category
    ds.decr_category_rnk(values['MainCategoryName'])
    ds.rem_category_name_idx(values['MainCategoryName'])

    # images
    img_ids = ds.get_product_image_ids(product_id)
    for img_id in img_ids:
        ds.rem_image(img_id)
    ds.rem_product_images(product_id)

    return json.dumps(deleted)


@app.route('/category/<category_id>', methods=['GET'])
def get_category(category_id=None):
    return json.dumps(ds.get_category(category_id))


@app.route('/image/<image_id>', methods=['GET'])
def get_image(image_id=None):
    return ds.get_image(image_id)


@app.route('/search/<term>', methods=['GET'])
def get_search(term):
    return json.dumps(ds.search_products(term))


@app.route('/list/<category_id>', methods=['GET'])
def get_list_by_category(category_id):
    return json.dumps(ds.list_products(category_id))


@app.route('/list', methods=['GET'])
def get_list():
    return json.dumps(ds.list_categories())


if __name__ == '__main__':
    app.run()
