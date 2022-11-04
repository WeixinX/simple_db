#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>

typedef struct {
    char *buffer;         // 保存一行内容的缓冲区
    size_t buffer_length; // 缓冲区长度
    ssize_t input_length; // 输入内容的长度
} InputBuffer;

InputBuffer *new_input_buffer() {
    InputBuffer *input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

void print_prompt() {
    printf("db > ");
}

void read_input(InputBuffer *input_buffer) {
    ssize_t bytes_read =
            getline(&input_buffer->buffer, &input_buffer->buffer_length, stdin);
    if (bytes_read <= 0) {
        printf("error reading input.\n");
        exit(EXIT_FAILURE);
    }

    // 忽略最后的换行符
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer *input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL

} ExecuteResult;

#define COLUMN_USERNAME_SIZE  32
#define COLUMN_EMAIL_SIZE 255
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct {
    StatementType type;
    Row row_to_insert;
} Statement;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

void print_row(Row *row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(Row *src, void *dst) {
    // 将 Row 结构体的数据紧凑放置到从 dest 开始的位置
    memcpy(dst + ID_OFFSET, &src->id, ID_SIZE);
    strncpy(dst + USERNAME_OFFSET, src->username, USERNAME_SIZE);
    strncpy(dst + EMAIL_OFFSET, src->email, EMAIL_SIZE);
}

void deserialize_row(void *src, Row *dst) {
    memcpy(&dst->id, src + ID_OFFSET, ID_SIZE);
    memcpy(dst->username, src + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(dst->email, src + EMAIL_OFFSET, EMAIL_SIZE);
}

#define TABLE_MAX_PAGES 100
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROWS_PER_PAGE;

typedef struct {
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
} Pager;

Pager *pager_open(const char *filename) {
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd == -1) {
        printf("unable to open file.\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager *pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void *get_page(Pager *pager, uint32_t page_num) {
    if (page_num >= TABLE_MAX_PAGES) {
        printf("tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }

        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t byte_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (byte_read == -1) {
                printf("error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}

void pager_flush(Pager *pager, uint32_t page_num, uint32_t size) {
    if (pager->pages[page_num] == NULL) {
        printf("tried to flush null page.\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("error seeking: %d.\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
    if (bytes_written == -1) {
        printf("error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

typedef struct {
    uint32_t num_row;
    Pager *pager;
} Table;

Table *db_open(const char *filename) {
    Pager *pager = pager_open(filename);
    uint32_t num_row = pager->file_length / ROW_SIZE;

    Table *table = malloc(sizeof(Table));
    table->pager = pager;
    table->num_row = num_row;
    return table;
}

void db_close(Table *table) {
    Pager *pager = table->pager;
    uint32_t num_full_pages = table->num_row / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < num_full_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        pager->pages[i] = NULL;
        free(pager->pages[i]);
    }

    uint32_t num_additional_rows = table->num_row % ROWS_PER_PAGE;
    if (num_additional_rows > 0) {
        uint32_t page_num = num_full_pages;
        if (pager->pages[page_num] != NULL) {
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
            pager->pages[page_num] = NULL;
            free(pager->pages[page_num]);
        }
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void *page = pager->pages[i];
        if (page) {
            pager->pages[i] = NULL;
            free(page);
        }
    }

    free(pager);
    free(table);
}

// row_slot 计算第 row_num 行数据的结束为止
void *row_slot(Table *table, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = get_page(table->pager, page_num);
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;

        char *id, *username, *email;
        char *str = strtok(input_buffer->buffer, " ");
        id = strtok(NULL, " ");
        username = strtok(NULL, " ");
        email = strtok(NULL, " ");

        if (id == NULL || username == NULL || email == NULL) {
            return PREPARE_SYNTAX_ERROR;
        }

        int id_num = atoi(id);
        if (id_num < 0) {
            return PREPARE_NEGATIVE_ID;
        }
        if (strlen(username) > COLUMN_USERNAME_SIZE) {
            return PREPARE_STRING_TOO_LONG;
        }
        if (strlen(email) > COLUMN_EMAIL_SIZE) {
            return PREPARE_STRING_TOO_LONG;
        }

        statement->row_to_insert.id = id_num;
        strcpy(statement->row_to_insert.username, username);
        strcpy(statement->row_to_insert.email, email);
        return PREPARE_SUCCESS;
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement *statement, Table *table) {
    if (table->num_row >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    Row *row_to_insert = &statement->row_to_insert;
    serialize_row(row_to_insert, row_slot(table, table->num_row));
    table->num_row += 1;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table) {
    Row row;
    for (uint32_t i = 0; i < table->num_row; i++) {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }
    char *filename = argv[1];
    Table *table = db_open(filename);
    InputBuffer *input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        // 处理系统指令
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("unrecognized command '%s'.\n", input_buffer->buffer);
                    continue;
            }
        }

        // 处理 SQL 语句
        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("syntax error. could not parse statement.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
                continue;
            case PREPARE_NEGATIVE_ID:
                printf("id must be positive.\n");
                continue;
            case PREPARE_STRING_TOO_LONG:
                printf("string is too long.\n");
                continue;
        }

        switch (execute_statement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("executed.\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("error: table full.\n");
                break;
        }
    }
}