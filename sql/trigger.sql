CREATE TABLE user_log_before(
    user_id BIGINT ,
    login VARCHAR(255) ,
    gravatar_id VARCHAR(255) ,
    url VARCHAR(255) ,
    avatar_url VARCHAR(255),
    state VARCHAR(255),
    log_timestamp timestamp(3) DEFAULT CURRENT_TIMESTAMP(3)
);
CREATE TABLE user_log_after(
    user_id BIGINT ,
    login VARCHAR(255) ,
    gravatar_id VARCHAR(255) ,
    url VARCHAR(255) ,
    avatar_url VARCHAR(255),
    state VARCHAR(255),
    log_timestamp timestamp(3) DEFAULT CURRENT_TIMESTAMP(3)
);

DELIMITER //

CREATE TRIGGER before_update_users
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id,login,gravatar_id,url,avatar_url,state,log_timestamp)
    VALUES(OLD.user_id, OLD.login, OLD.gravatar_id,OLD.url,OLD.avatar_url,"UPDATE",NOW());
END //

CREATE TRIGGER after_update_users
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id,login,gravatar_id,url,avatar_url,state,log_timestamp)
    VALUES(NEW.user_id, NEW.login, NEW.gravatar_id,NEW.url,NEW.avatar_url,"UPDATE",NOW());
END //

CREATE TRIGGER before_insert_users
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id,login,gravatar_id,url,avatar_url,state,log_timestamp)
    VALUES(NEW.user_id, NEW.login, NEW.gravatar_id,NEW.url,NEW.avatar_url,"INSERT",NOW());
END //

CREATE TRIGGER after_insert_users
AFTER INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id,login,gravatar_id,url,avatar_url,state,log_timestamp)
    VALUES(NEW.user_id, NEW.login, NEW.gravatar_id,NEW.url,NEW.avatar_url,"INSERT",NOW());
END //
CREATE TRIGGER before_delete_users
BEFORE DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id,login,gravatar_id,url,avatar_url,state,log_timestamp)
    VALUES(OLD.user_id, OLD.login, OLD.gravatar_id,OLD.url,OLD.avatar_url,"DELETE",NOW());
END //
CREATE TRIGGER after_delete_users
AFTER DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id,login,gravatar_id,url,avatar_url,state,log_timestamp)
    VALUES(OLD.user_id, OLD.login, OLD.gravatar_id,OLD.url,OLD.avatar_url,"DELETE",NOW());
END //
DELIMITER ;


-- insert data
INSERT INTO users (user_id, login, gravatar_id, url, avatar_url)
VALUES (1, 'alice', 'grav1', 'http://example.com/alice', 'http://img.com/alice.png');

INSERT INTO users (user_id, login, gravatar_id, url, avatar_url)
VALUES (2, 'bob', 'grav2', 'http://example.com/bob', 'http://img.com/bob.png');

INSERT INTO users (user_id, login, gravatar_id, url, avatar_url)
VALUES (3, 'charlie', 'grav3', 'http://example.com/charlie', 'http://img.com/charlie.png');

INSERT INTO users (user_id, login, gravatar_id, url, avatar_url)
VALUES (4, 'david', 'grav4', 'http://example.com/david', 'http://img.com/david.png');

INSERT INTO users (user_id, login, gravatar_id, url, avatar_url)
VALUES (5, 'eva', 'grav5', 'http://example.com/eva', 'http://img.com/eva.png');

INSERT INTO users (user_id, login, gravatar_id, url, avatar_url)
VALUES (6, 'frank', 'grav6', 'http://example.com/frank', 'http://img.com/frank.png');

INSERT INTO users (user_id, login, gravatar_id, url, avatar_url)
VALUES (7, 'grace', 'grav7', 'http://example.com/grace', 'http://img.com/grace.png');

INSERT INTO users (user_id, login, gravatar_id, url, avatar_url)
VALUES (8, 'henry', 'grav8', 'http://example.com/henry', 'http://img.com/henry.png');

INSERT INTO users (user_id, login, gravatar_id, url, avatar_url)
VALUES (9, 'irene', 'grav9', 'http://example.com/irene', 'http://img.com/irene.png');

INSERT INTO users (user_id, login, gravatar_id, url, avatar_url)
VALUES (10, 'jack', 'grav10', 'http://example.com/jack', 'http://img.com/jack.png');

-- update data
UPDATE users SET login = 'alice_new' WHERE user_id = 1;
UPDATE users SET avatar_url = 'http://img.com/bob_new.png' WHERE user_id = 2;
UPDATE users SET url = 'http://newsite.com/charlie' WHERE user_id = 3;
UPDATE users SET gravatar_id = 'grav44' WHERE user_id = 4;
UPDATE users SET login = 'eva99', url = 'http://eva99.com' WHERE user_id = 5;
UPDATE users SET avatar_url = 'http://cdn.com/frank_avatar.png' WHERE user_id = 6;
UPDATE users SET login = UPPER(login) WHERE user_id = 7;
UPDATE users SET gravatar_id = CONCAT(gravatar_id, '_updated') WHERE user_id = 8;
UPDATE users SET login = 'irene_new', avatar_url = 'http://imgnew.com/irene.png' WHERE user_id = 9;
UPDATE users SET url = 'http://jack-blog.com', gravatar_id = 'jackgrav' WHERE user_id = 10;

-- delte data
DELETE FROM users WHERE user_id = 1;
DELETE FROM users WHERE user_id = 2;
DELETE FROM users WHERE user_id = 3;
