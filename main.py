import json

from redis import StrictRedis
from flask import Flask
from flask import request

from datastore import DataStore


app = Flask(__name__)
DS = DataStore(StrictRedis(decode_responses=True, host="localhost", port=6379))


@app.route('/product/<product_id>', methods=['GET'])
def get_product(product_id):

    # get product
    product = DS.get_product(product_id)

    return json.dumps(product)


@app.route('/product', methods=['POST'])
def post_product():

    # parse request body
    values = json.loads(request.data)

    # generate product id
    product_id = DS.new_product_id()

    # set category
    category_id = DS.get_category_id(values['MainCategoryName'])
    if not category_id:
        category_id = DS.new_category_id()
        DS.set_category(category_id, values['MainCategoryName'])
    DS.add_product_to_category(product_id, category_id)
    DS.set_category_name_idx(values['MainCategoryName'], category_id)
    DS.incr_category_rnk(values['MainCategoryName'])

    # set images
    img_ids = []
    for img in values['Images']:
        img_id = DS.new_image_id()
        img_ids.append(img_id)
        DS.set_image(img_id, img)
    DS.add_product_image_ids(img_ids, product_id)
    values['Images'] = 'product:{}:images'.format(product_id)

    # set product
    DS.set_product_name_idx(values['Name'], product_id)
    DS.set_product(product_id, values)

    return json.dumps(product_id)


@app.route('/product/<product_id>', methods=['PUT'])
def put_product(product_id):

    # product to update to
    new_values = json.loads(request.data)

    # product to be updated
    old_values = DS.get_product(product_id)

    # check category
    if new_values['MainCategoryName'] != old_values['MainCategoryName']:

        old_category_id = DS.get_category_id(old_values['MainCategoryName'])

        # remove old category
        category_id = old_category_id
        category = DS.get_category(category_id)
        DS.decr_category_rnk(category['Name'])
        DS.rem_category_name_idx(category['Name'])
        DS.rem_product_from_category(product_id, category_id)

        # add new category
        category_id = DS.get_category_id(new_values['MainCategory'])
        if not category_id:
            category_id = DS.new_category_id()
            DS.set_category(category_id, new_values['MainCategoryName'])
        DS.set_category_name_idx(new_values['MainCategoryName'], category_id)
        DS.incr_category_rnk(new_values['MainCategoryName'])
        DS.add_product_to_category(product_id, category_id)
        new_values['MainCategory'] = category_id

    # check images
    if set(new_values['Images']) != set(old_values['Images']):

        # remove old images
        imgs_to_remove = set(old_values['Images']) - set(new_values['Images'])
        for img in imgs_to_remove:
            img_id = DS.get_image_id(img)
            DS.rem_image(img_id)
            DS.rem_images_from_product(product_id, img_id)

        # add new images
        new_imgs = set(new_values['Images']) - set(old_values['Images'])
        for img in new_imgs:
            img_id = DS.new_image_id()
            DS.set_image(img_id, img)
            DS.add_product_image_ids(product_id, img_id)
        new_values['Images'] = 'product:{}:images'.format(product_id)

    # set product
    DS.set_product_name_idx(new_values['Name'], product_id)
    updated = DS.set_product(product_id, new_values)

    return json.dumps(updated)


@app.route('/product/<product_id>', methods=['DELETE'])
def delete_prodcut(product_id):

    # product to be deleted
    values = DS.get_product(product_id)

    # product
    deleted = DS.rem_product(product_id)
    category_id = DS.get_category_id(values['MainCategoryName'])
    DS.rem_product_from_category(product_id, category_id)
    DS.rem_product_name_idx(values['Name'])

    # category
    DS.decr_category_rnk(values['MainCategoryName'])
    DS.rem_category_name_idx(values['MainCategoryName'])

    # images
    img_ids = DS.get_product_image_ids(product_id)
    [DS.rem_image(img_id) for img_id in img_ids]
    DS.rem_product_images(product_id)

    return json.dumps(deleted)


@app.route('/category/<category_id>', methods=['GET'])
def get_category(category_id=None):
    return json.dumps(DS.get_category(category_id))


@app.route('/image/<image_id>', methods=['GET'])
def get_image(image_id=None):
    return DS.get_image(image_id)


@app.route('/search/<term>', methods=['GET'])
def get_search(term):
    return json.dumps(DS.search_products(term))


@app.route('/list/<category_id>', methods=['GET'])
def get_list_by_category(category_id):
    return json.dumps(DS.list_products(category_id))


@app.route('/list', methods=['GET'])
def get_list():
    return json.dumps(DS.list_categories())


if __name__ == '__main__':
    app.run()
