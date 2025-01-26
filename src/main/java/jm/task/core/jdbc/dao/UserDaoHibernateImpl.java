package jm.task.core.jdbc.dao;
import jm.task.core.jdbc.model.User;
import java.util.List;
import jm.task.core.jdbc.util.Util;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

public class UserDaoHibernateImpl implements UserDao {
    public UserDaoHibernateImpl() {
    }
    private static final Logger logger = LoggerFactory.getLogger(UserDaoHibernateImpl.class);

    @Override
    public void createUsersTable() {
        Transaction transaction = null;
        String sql = "CREATE TABLE IF NOT EXISTS users (" +
                "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                "name VARCHAR(50), " +
                "lastName VARCHAR(50), " +
                "age TINYINT)";
        try (org.hibernate.Session session = Util.getSessionFactory().openSession()) {
            transaction = session.beginTransaction();
            session.createSQLQuery(sql).executeUpdate();
            transaction.commit();
            logger.info("Таблица 'users' успешно создана или уже существует.");
        } catch (Exception e) {
            if (transaction != null) transaction.rollback();
            logger.error("Ошибка при создании таблицы 'users'.", e);
            throw new DaoException("Ошибка при создании таблицы 'users'.", e);
        }
    }

    @Override
    public void dropUsersTable() {
        Transaction transaction = null;
        String sql = "DROP TABLE IF EXISTS users";
        try (org.hibernate.Session session = Util.getSessionFactory().openSession()) {
            transaction = session.beginTransaction();
            session.createSQLQuery(sql).executeUpdate();
            transaction.commit();
            logger.info("Таблица 'users' успешно удалена или не существовала.");
        } catch (Exception e) {
            if (transaction != null) transaction.rollback();
            logger.error("Ошибка при удалении таблицы 'users'.", e);
            throw new DaoException("Ошибка при удалении таблицы 'users'.", e);
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        Transaction transaction = null;
        try (org.hibernate.Session session = Util.getSessionFactory().openSession()) {
            transaction = session.beginTransaction();
            User user = new User(name, lastName, age);
            session.save(user);
            transaction.commit();
            logger.info("User с именем – {} добавлен в базу данных.", name);
        } catch (Exception e) {
            if (transaction != null) transaction.rollback();
            logger.error("Ошибка при добавлении пользователя с именем – {}.", name, e);
            throw new DaoException("Ошибка при добавлении пользователя.", e);
        }
    }

    @Override
    public void removeUserById(long id) {
        Transaction transaction = null;
        try (org.hibernate.Session session = Util.getSessionFactory().openSession()) {
            transaction = session.beginTransaction();
            User user = session.get(User.class, id);
            if (user != null) {
                session.delete(user);
                logger.info("Пользователь с id = {} удален из базы данных.", id);
            } else {
                logger.warn("Пользователь с id = {} не найден.", id);
            }
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) transaction.rollback();
            logger.error("Ошибка при удалении пользователя с id = {}.", id, e);
            throw new DaoException("Ошибка при удалении пользователя.", e);
        }
    }


    @Override
    public List<User> getAllUsers() {
        try (org.hibernate.Session session = Util.getSessionFactory().openSession()) {
            List<User> users = session.createQuery("FROM User", User.class).list();
            logger.info("Получено {} пользователей из базы данных.", users.size());
            return users;
        } catch (Exception e) {
            logger.error("Ошибка при получении списка пользователей.", e);
            throw new DaoException("Ошибка при получении списка пользователей.", e);
        }
    }

    @Override
    public void cleanUsersTable() {
        Transaction transaction = null;
        String hql = "DELETE FROM User";
        try (Session session = Util.getSessionFactory().openSession()) {
            transaction = session.beginTransaction();
            Query query = session.createQuery(hql);
            int deletedRows = query.executeUpdate();
            transaction.commit();
            logger.info("Таблица 'users' очищена, удалено {} записей.", deletedRows);
        } catch (Exception e) {
            if (transaction != null) transaction.rollback();
            logger.error("Ошибка при очистке таблицы 'users'.", e);
            throw new DaoException("Ошибка при очистке таблицы 'users'.", e);
        }
    }
}