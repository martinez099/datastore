import json

from redis import StrictRedis
from flask import request

from flask import Flask
app = Flask(__name__)

r = StrictRedis(decode_responses=True)


"""
Product Image

Id : Number
Value : Binary

Product

Id : Number
Name : String
Description: String
Vendor : String
Price : Number
Currency : String
MainCategory : Category (1)
Images : Image (0..n)

Category

Id : Number
Name : String
Products : Product (0..n)
"""


def insert_category(name):
    category_id = r.incr('ID:CATEGORY')
    if r.set('CATEGORY:{}'.format(category_id), name):
        return category_id


def rem_category(category_id):
    return bool(r.delete('CATEGORY:{}'.format(category_id)))


def insert_image(value):
    image_id = r.incr('ID:IMAGE')
    if r.set('IMAGE:{}'.format(image_id), value):
        return image_id


def rem_image(img_id):
    return bool(r.delete('IMAGE:{}'.format(img_id)))


def add_product_to_category(product_id, category_id):
    return bool(r.lpush('PRODUCTS_IN_CATEGORY:{}'.format(category_id), product_id))


def rem_product_from_category(product_id, category_id):
    return bool(r.lrem('PRODUCTS_IN_CATEGORY:{}'.format(category_id), 1, product_id))


def get_category_id(name):
    return r.hget('IDX:CATEGORY_NAME', name)


def add_images_of_product(image_ids, product_id):
    return bool(r.lpush('IMAGES_OF_PRODUCT:{}'.format(product_id), *image_ids))


def get_images_of_prduct(product_id):
    return r.lrange('IMAGES_OF_PRODUCT:{}'.format(product_id), 0, -1)


def rem_imgages_of_product(product_id):
    return bool(r.delete('IMAGES_OF_PRODUCT:{}'.format(product_id)))


def add_product_name_idx(product_name, product_id):
    return bool(r.hset('IDX:PRODUCT_NAME', product_name, product_id))


def rem_product_name_idx(product_name):
    return bool(r.hdel('IDX:PRODUCT_NAME', product_name))


def incr_category_cnt(category_name):
    return bool(r.zincrby('CNT:PRODUCT_IN_CATEGORY', category_name))


def decr_category_cnt(category_name):
    return bool(r.zincrby('CNT:PRODUCT_IN_CATEGORY', category_name, amount=-1))


def add_category_name_idx(category_name, category_id):
    return bool(r.hset('IDX:CATEGORY_NAME', category_name, category_id))


def rem_category_name_idx(category_name):
    return bool(r.hdel('IDX:CATEGORY_NAME', category_name))


def get_product(product_id, resolve=False):
    prod = r.hgetall('PRODUCT:{}'.format(product_id))
    if not prod:
        return None
    if resolve:
        prod['MainCategory'] = r.get('CATEGORY:{}'.format(prod['MainCategory']))
        prod['Images'] = [r.get('IMAGE:{}'.format(i)) for i in r.lrange('IMAGES_OF_PRODUCT:{}'.format(product_id), 0, -1)]
    return prod


def insert_product(values):

    # category
    category_id = get_category_id(values['MainCategory'])
    if not category_id:
        category_id = insert_category(values['MainCategory'])
    add_category_name_idx(values['MainCategory'], category_id)
    incr_category_cnt(values['MainCategory'])
    values['MainCategory'] = category_id

    # images
    img_ids = [insert_image(img) for img in values['Images']]
    del values['Images']

    # product
    product_id = r.incr('ID:PRODUCT')
    add_images_of_product(img_ids, product_id)
    add_product_to_category(product_id, category_id)
    add_product_name_idx(values['Name'], product_id)
    r.hmset('PRODUCT:{}'.format(product_id), values)
    return product_id


def delete_product(product_id):

    # product to be deleted
    values = get_product(product_id)

    # images
    img_ids = get_images_of_prduct(product_id)
    for img_id in img_ids:
        rem_image(img_id)

    # category
    rem_product_from_category(product_id, values['MainCategory'])
    decr_category_cnt(get_category(values['MainCategory']))
    rem_category_name_idx(get_category(values['MainCategory']))

    # product
    rem_product_name_idx(values['Name'])
    rem_imgages_of_product(product_id)

    return bool(r.delete('PRODUCT:{}'.format(product_id)))


def update_product(product_id, new_values):

    # product to be updated
    old_values = get_product(product_id)

    # category
    decr_category_cnt(get_category(old_values['MainCategory']))
    rem_category_name_idx(get_category(old_values['MainCategory']))
    category_id = get_category_id(new_values['MainCategory'])
    if not category_id:
        category_id = insert_category(new_values['MainCategory'])
    add_category_name_idx(new_values['MainCategory'], category_id)
    incr_category_cnt(new_values['MainCategory'])
    new_values['MainCategory'] = category_id

    # images
    for img in get_images_of_prduct(product_id):
        rem_image(img)

    img_ids = [insert_image(img) for img in new_values['Images']]
    del new_values['Images']

    # product
    rem_imgages_of_product(product_id)
    rem_product_name_idx(old_values['Name'])
    rem_product_from_category(product_id, old_values['MainCategory'])
    add_images_of_product(img_ids, product_id)
    add_product_to_category(product_id, category_id)
    add_product_name_idx(new_values['Name'], product_id)

    return bool(r.hmset('PRODUCT:{}'.format(product_id), new_values))


@app.route('/product', methods=['POST'])
@app.route('/product/<product_id>', methods=['GET', 'PUT', 'DELETE'])
def product(product_id=None):

    def do_get():
        values = get_product(product_id, resolve=True)
        return json.dumps(values)

    def do_post():
        values = json.loads(request.data)
        return insert_product(values)

    def do_delete():
        return delete_product(product_id)

    def do_put():
        values = json.loads(request.data)
        return update_product(product_id, values)

    handler = {'GET': do_get,
               'POST': do_post,
               'DELETE': do_delete,
               'PUT': do_put,
              }[request.method]

    return str(handler())


def get_category(category_id):
    return r.get('CATEGORY:{}'.format(category_id))


@app.route('/category/<category_id>', methods=['GET'])
def category(category_id=None):
    return get_category(category_id)


def get_image(image_id):
    return r.get('IMAGE:{}'.format(image_id))

@app.route('/image/<image_id>', methods=['GET'])
def image(image_id=None):
    return get_image(image_id)


def search_product(term):
    return [r.hgetall('PRODUCT:{}'.format(idx[1])) for idx in r.hscan_iter('IDX:PRODUCT_NAME', '*{}*'.format(term))]


@app.route('/search/<term>', methods=['GET'])
def search(term):
    return json.dumps(search_product(term))


def list_by_category(category_id):
    return [r.hgetall('PRODUCT:{}'.format(idx)) for idx in r.lrange('PRODUCTS_IN_CATEGORY:{}'.format(category_id), 0, -1)]


def list_chart():
    return r.zrangebyscore('CNT:PRODUCT_IN_CATEGORY', '-Inf', '+Inf')


@app.route('/list', methods=['GET'])
@app.route('/list/<category_id>', methods=['GET'])
def list(category_id=None):
    if category_id:
        return json.dumps(list_by_category(category_id))
    else:
        return json.dumps(list_chart())
