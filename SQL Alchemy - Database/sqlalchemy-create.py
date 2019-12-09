from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Numeric, SmallInteger, or_, and_, not_, desc, \
    func, distinct, text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pprint import pprint

# Create an engine that stores data in the local directory's
engine = create_engine('sqlite:////web/SQL Alchemy - sqllite data/sqlalchemy_ruch.db')

# this loads the sqlalchemy base class
Base = declarative_base()


# Setting up the classes that create the record objects and define the schema


class Customers(Base):
    __tablename__ = 'customers'
    id = Column(Integer, primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    username = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False)
    address = Column(String(250), nullable=False)
    town = Column(String(100), nullable=False)
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)
    orders = relationship("Orders", backref='customer')


class Items(Base):
    __tablename__ = 'items'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    cost_price = Column(Numeric(10, 2), nullable=False)
    selling_price = Column(Numeric(10, 2), nullable=False)
    quantity = Column(Integer, nullable=False)


class Orders(Base):
    __tablename__ = 'orders'
    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer(), ForeignKey('customers.id'))
    date_placed = Column(DateTime(), default=datetime.now)
    date_shipped = Column(DateTime)
    line_items = relationship("OrderLines", backref='order')


class OrderLines(Base):
    __tablename__ = 'order_lines'
    id = Column(Integer(), primary_key=True)
    order_id = Column(Integer(), ForeignKey('orders.id'))
    item_id = Column(Integer(), ForeignKey('items.id'))
    quantity = Column(SmallInteger())
    item = relationship("Items")


# Create all tables in the engine. This is equivalent to "Create Table"
# statements in raw SQL.
Base.metadata.create_all(engine)

Base.metadata.bind = engine

DBSession = sessionmaker(bind=engine)
# A DBSession() instance establishes all conversations with the database
# and represents a "staging zone" for all the objects loaded into the
# database session object. Any change made against the objects in the
# session won't be persisted into the database until you call
# session.commit(). If you're not happy about the changes, you can
# revert all of them back to the last commit by calling
# session.rollback()
session = DBSession()

# Insert Customers in the customers table
c1 = Customers(first_name='Toby',
               last_name='Miller',
               username='tmiller',
               email='tmiller@example.com',
               address='1662 Kinney Street',
               town='Wolfden')

c2 = Customers(first_name='Scott',
               last_name='Harvey',
               username='scottharvey',
               email='scottharvey@example.com',
               address='424 Patterson Street',
               town='Beckinsdale')

c3 = Customers(first_name="John",
               last_name="Lara",
               username="johnlara",
               email="johnlara@mail.com",
               address="3073 Derek Drive",
               town="Norfolk")

c4 = Customers(first_name="Sarah",
               last_name="Tomlin",
               username="sarahtomlin",
               email="sarahtomlin@mail.com",
               address="3572 Poplar Avenue",
               town="Norfolk")

c5 = Customers(first_name='Toby',
               last_name='Miller',
               username='tmiller',
               email='tmiller@example.com',
               address='1662 Kinney Street',
               town='Wolfden')

c6 = Customers(first_name='Scott',
               last_name='Harvey',
               username='scottharvey',
               email='scottharvey@example.com',
               address='424 Patterson Street',
               town='Beckinsdale')

session.add_all([c1, c2, c3, c4, c5, c6])
session.commit()

i1 = Items(name='Chair', cost_price=9.21, selling_price=10.81, quantity=5)
i2 = Items(name='Pen', cost_price=3.45, selling_price=4.51, quantity=3)
i3 = Items(name='Headphone', cost_price=15.52, selling_price=16.81, quantity=50)
i4 = Items(name='Travel Bag', cost_price=20.1, selling_price=24.21, quantity=50)
i5 = Items(name='Keyboard', cost_price=20.1, selling_price=22.11, quantity=50)
i6 = Items(name='Monitor', cost_price=200.14, selling_price=212.89, quantity=50)
i7 = Items(name='Watch', cost_price=100.58, selling_price=104.41, quantity=50)
i8 = Items(name='Water Bottle', cost_price=20.89, selling_price=25, quantity=50)

session.add_all([i1, i2, i3, i4, i5, i6, i7, i8])
session.commit()

o1 = Orders(customer=c1)
o2 = Orders(customer=c1)

line_item1 = OrderLines(order=o1, item=i1, quantity=3)
line_item2 = OrderLines(order=o1, item=i2, quantity=2)
line_item3 = OrderLines(order=o2, item=i1, quantity=1)
line_item3 = OrderLines(order=o2, item=i2, quantity=4)

session.add_all([o1, o2])

session.new
session.commit()

all_customers = session.query(Customers).all()
for customers in all_customers:
    pprint(customers.__dict__)

all_items = session.query(Items).all()
for items in all_items:
    pprint(items.__dict__)

all_orders = session.query(Orders).all()
for orders in all_orders:
    pprint(orders.__dict__)

q = session.query(Customers)

for c in q:
    print(c.id, c.first_name)

print(repr(session.query(Customers.id, Customers.first_name).all()))

print(repr(session.query(Customers).count()))
print(repr(session.query(Items).count()))
print(repr(session.query(Orders).count()))

print(repr(session.query(Customers).first()))
print(repr(session.query(Items).first()))
print(repr(session.query(Orders).first()))

print(repr(session.query(Customers).get(1)))
print(repr(session.query(Items).get(1)))
print(repr(session.query(Orders).get(100)))

print(repr(session.query(Customers).filter(Customers.first_name == 'John').all()))

# find all customers who either live in Peterbrugh or Norfolk

print(repr(session.query(Customers).filter(or_(
    Customers.town == 'Peterbrugh',
    Customers.town == 'Norfolk'
)).all()))

print(repr(session.query(Customers).filter(and_(
    Customers.first_name == 'John',
    Customers.town == 'Norfolk'
)).all()))

print(repr(session.query(Customers).filter(and_(
    Customers.first_name == 'John',
    not_(
        Customers.town == 'Peterbrugh',
    )
)).all()))

print(repr(session.query(Orders).filter(Orders.date_shipped == None).all()))
print(repr(session.query(Orders).filter(Orders.date_shipped != None).all()))
print(repr(session.query(Customers).filter(Customers.first_name.in_(['Toby', 'Sarah'])).all()))
print(repr(session.query(Customers).filter(Customers.first_name.notin_(['Toby', 'Sarah'])).all()))

print(repr(session.query(Items).filter(Items.cost_price.between(10, 50)).all()))
print(repr(session.query(Items).filter(not_(Items.cost_price.between(10, 50))).all()))
print(repr(session.query(Items).filter(Items.name.like("%r")).all()))
print(repr(session.query(Items).filter(not_(Items.name.like("W%"))).all()))

print(repr(session.query(Customers).limit(2).all()))
print(repr(session.query(Customers).filter(Customers.address.ilike("%avenue")).limit(2).all()))

print(repr(session.query(Customers).limit(2).offset(2).all()))

print(repr(session.query(Items).filter(Items.name.ilike("wa%")).all()))
print(repr(session.query(Items).filter(Items.name.ilike("wa%")).order_by(Items.cost_price).all()))
print(repr(session.query(Items).filter(Items.name.ilike("wa%")).order_by(desc(Items.cost_price)).all()))

join_results = session.query(Customers).join(Orders).all()
print(repr(join_results))

print(repr(session.query(
    Customers.first_name,
    Orders.id,
).outerjoin(Orders).all()
           ))

print(repr(session.query(func.count(Customers.id)).join(Orders).filter(
    Customers.first_name == 'John',
    Customers.last_name == 'Green',
).group_by(Customers.id).scalar()
           ))

print(repr(session.query(
    func.count("*").label('town_count'),
    Customers.town
).group_by(Customers.town).having(func.count("*") > 2).all()))

print(repr(session.query(Customers.town).filter(Customers.id < 10).all()))
print(repr(session.query(Customers.town).filter(Customers.id < 10).distinct().all()))

print(repr(session.query(
    func.count(distinct(Customers.town)),
    func.count(Customers.town)
).all()))

s1 = session.query(Items.id, Items.name).filter(Items.name.like("Wa%"))
s2 = session.query(Items.id, Items.name).filter(Items.name.like("%e%"))
print(repr(s1.union(s2).all()))

i = session.query(Items).get(8)
i.selling_price = 25.91
session.add(i)
session.commit()

session.query(Items).filter(
    Items.name.ilike("W%")
).update({"quantity": 60}, synchronize_session='fetch')
session.commit()

i = session.query(Items).filter(Items.name == 'Monitor').one()
print(repr(i))
session.delete(i)
session.commit()

session.query(Items).filter(
    Items.name.ilike("W%")
).delete(synchronize_session='fetch')
session.commit()

print(repr(session.query(Customers).filter(text("first_name = 'John'")).all()))
print(repr(session.query(Customers).filter(text("town like 'Nor%'")).all()))
print(repr(session.query(Customers).filter(text("town like 'Nor%'")).order_by(text("first_name, id desc")).all()))
