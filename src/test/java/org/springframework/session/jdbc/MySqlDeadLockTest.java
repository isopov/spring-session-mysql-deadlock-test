package org.springframework.session.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.testcontainers.containers.MySQLContainer;

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MySqlDeadLockTest {

    @Rule
    public MySQLContainer mysql = new MySQLContainer();

    private HikariDataSource ds;
    private JdbcTemplate template;
    private JdbcOperationsSessionRepository repository;



    @Before
    public void setup() {
        System.out.println("Url " + mysql.getJdbcUrl());
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setMaximumPoolSize(INSERTING_THREADS + 1);
        hikariConfig.setJdbcUrl(mysql.getJdbcUrl());
        hikariConfig.setUsername(mysql.getUsername());
        hikariConfig.setPassword(mysql.getPassword());

        ds = new HikariDataSource(hikariConfig);
        DataSourceTransactionManager tm = new DataSourceTransactionManager(ds);
        template = new JdbcTemplate(ds);
        repository = new JdbcOperationsSessionRepository(template, tm);


        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource("/org/springframework/session/jdbc/schema-mysql.sql"));
        populator.setContinueOnError(true);
        DatabasePopulatorUtils.execute(populator, ds);
    }


    @After
    public void tearDown() {
        ds.close();
    }

    private static final int INSERTING_THREADS = 10;
    private static final int ACTIVE_SESSIONS = 10000;
    private static final int SESSIONS = 1_000;
    private static final int STEPS = 1_000;

    @Test
    public void testDeadLock() throws InterruptedException {
        for (int i = 0; i < ACTIVE_SESSIONS; i++) {
            JdbcOperationsSessionRepository.JdbcSession session = repository.createSession();
            repository.save(session);
        }

        ExecutorService pool = Executors.newFixedThreadPool(INSERTING_THREADS + 1);
        LinkedBlockingQueue<Object> deletes = new LinkedBlockingQueue<>();
        for (int k = 0; k < INSERTING_THREADS; k++) {
            int kk = k;
            pool.submit(() -> {
                for (int j = 0; j < STEPS; j++) {
                    for (int i = 0; i < SESSIONS; i++) {
                        JdbcOperationsSessionRepository.JdbcSession session = repository.createSession();
                        session.setLastAccessedTime(Instant.EPOCH);
                        repository.save(session);
                    }
                    System.out.println(kk + " inserted step " + j);
                    try {
                        deletes.put(new Object());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            });
        }

        pool.submit(() -> {
            for (int j = 0; j < STEPS; j++) {
                try {
                    for (int i = 0; i < INSERTING_THREADS; i++) {
                        deletes.take();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                repository.cleanUpExpiredSessions();
                System.out.println("Deleted step " + j);
            }
        });

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.MINUTES);
    }

    public static void main(String[] args) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        LinkedBlockingQueue<Object> deletes = new LinkedBlockingQueue<>(100);
        pool.submit(() -> {
            for (int j = 0; j < STEPS; j++) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Inserted step " + j);
                try {
                    deletes.put(new Object());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });

        pool.submit(() -> {
            for (int j = 0; j < STEPS; j++) {
                try {
                    deletes.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Deleted step " + j);
            }
        });

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.MINUTES);
    }


}
